#include "data_recorder/concurrent_recorder.h"
#include <ros/ros.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <std_msgs/Float64MultiArray.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <sensor_msgs/PointCloud2.h>
#include <sqlite3.h>
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <filesystem>
#include <chrono>
#include <ctime>
#include <functional>

namespace fs = std::filesystem;

// 全局关闭标志
std::atomic<bool> g_shutdown_requested{false};

void signalHandler(int signal)
{
    g_shutdown_requested = true;
    ros::shutdown();
}

ConcurrentRecorder::ConcurrentRecorder() : nh_("~"), db_(nullptr)
{
    // 加载配置
    std::string config_file;
    nh_.param("config_file", config_file, std::string("config.yaml"));
    loadConfig(config_file);

    // 获取数据目录
    nh_.param("data_dir", base_dir_, std::string("/data/ros_recordings"));
    db_path_ = base_dir_ + "/recordings.db";
    
    // 创建数据目录
    if (!fs::exists(base_dir_))
    {
        fs::create_directories(base_dir_);
    }
    
    // 初始化数据库
    initDatabase();
    
    // 订阅图像话题
    for (const auto& topic : image_topics_) {
        ros::Subscriber sub = nh_.subscribe<sensor_msgs::CompressedImage>(
            topic, 100, 
            [this, topic](const sensor_msgs::CompressedImage::ConstPtr& msg) {
                this->imageCallback(msg, topic);
            }
        );
        image_subs_.push_back(sub);
        ROS_INFO("Subscribed to image topic: %s", topic.c_str());
    }
    
    // 订阅点云话题
    for (const auto& topic : point_cloud_topics_) {
        ros::Subscriber sub = nh_.subscribe<sensor_msgs::PointCloud2>(
            topic, 100, 
            [this, topic](const sensor_msgs::PointCloud2::ConstPtr& msg) {
                this->pointCloudCallback(msg, topic);
            }
        );
        point_cloud_subs_.push_back(sub);
        ROS_INFO("Subscribed to point cloud topic: %s", topic.c_str());
    }
    
    // 启动工作线程
    recording_thread_ = std::thread(&ConcurrentRecorder::recordingWorker, this);
    db_maintain_thread_ = std::thread(&ConcurrentRecorder::dbMaintainWorker, this);
    
    ROS_INFO("Bag recorder initialized with data directory: %s", base_dir_.c_str());
}

ConcurrentRecorder::~ConcurrentRecorder()
{
    stop();
}

void ConcurrentRecorder::stop()
{
    stop_threads_ = true;
    message_cv_.notify_all();
    
    if (recording_thread_.joinable()) recording_thread_.join();
    if (db_maintain_thread_.joinable()) db_maintain_thread_.join();

    // 确保当前bag段被正确关闭
    if (current_bag_.isOpen()) {
        finalizeCurrentBagSegment(ros::Time::now().toSec());
    }

    if (db_) {
        sqlite3_close(db_);
    }
    
    ROS_INFO("Bag recorder stopped. Recorded %lu messages.", messages_recorded_.load());
}

void ConcurrentRecorder::run()
{
    ros::Rate rate(10);
    while (ros::ok() && !g_shutdown_requested)
    {
        ros::spinOnce();
        rate.sleep();
    }
    stop();
}

void ConcurrentRecorder::imageCallback(const sensor_msgs::CompressedImage::ConstPtr& msg, const std::string& topic)
{
    ImageMessage image_msg;
    image_msg.msg = *msg;
    image_msg.topic = topic;
    
    std::lock_guard<std::mutex> lock(message_mutex_);
    message_queue_.push(image_msg);
    message_cv_.notify_one();
}

void ConcurrentRecorder::pointCloudCallback(const sensor_msgs::PointCloud2::ConstPtr& msg, const std::string& topic)
{
    PointCloudMessage point_cloud_msg;
    point_cloud_msg.msg = *msg;
    point_cloud_msg.topic = topic;
    
    std::lock_guard<std::mutex> lock(message_mutex_);
    message_queue_.push(point_cloud_msg);
    message_cv_.notify_one();
}

void ConcurrentRecorder::startNewBagSegment(double start_timestamp)
{
    // 生成bag文件名
    std::time_t t = static_cast<std::time_t>(start_timestamp);
    std::tm* tm = std::localtime(&t);
    char filename[100];
    std::strftime(filename, sizeof(filename), "%Y%m%d_%H%M%S", tm);
    
    current_bag_path_ = base_dir_ + "/" + std::string(filename) + ".bag";
    current_bag_start_time_ = start_timestamp;
    
    try {
        current_bag_.open(current_bag_path_, rosbag::bagmode::Write);
        ROS_INFO("Starting new bag segment: %s", current_bag_path_.c_str());
    } catch (rosbag::BagException& e) {
        ROS_ERROR("Failed to open bag file: %s", e.what());
    }
}

void ConcurrentRecorder::finalizeCurrentBagSegment(double end_timestamp)
{
    if (current_bag_.isOpen()) {
        current_bag_.close();
        
        // 记录到数据库
        sqlite3_stmt* stmt;
        const char* sql = "INSERT INTO bag_segments (bag_path, start_time, end_time) VALUES (?, ?, ?)";
        
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, current_bag_path_.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_double(stmt, 2, current_bag_start_time_);
            sqlite3_bind_double(stmt, 3, end_timestamp);
            
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                ROS_ERROR("Failed to insert bag segment into database: %s", sqlite3_errmsg(db_));
            }
            
            sqlite3_finalize(stmt);
        }
        
        ROS_INFO("Finalized bag segment: %s", current_bag_path_.c_str());
    }
}

void ConcurrentRecorder::recordingWorker()
{
    ROS_INFO("Recording worker started");
    
    while (!stop_threads_ && !g_shutdown_requested && ros::ok())
    {
        MessageVariant message;
        {
            std::unique_lock<std::mutex> lock(message_mutex_);
            message_cv_.wait_for(lock, std::chrono::milliseconds(100), 
                [this] { return !message_queue_.empty() || stop_threads_; });
            
            if (stop_threads_) break;
            
            if (message_queue_.empty()) continue;
            
            message = message_queue_.front();
            message_queue_.pop();
        }
        
        // 处理录制消息
        processRecordingMessage(message);
        
        // 检查是否需要创建新的bag段
        double current_time = ros::Time::now().toSec();
        if (current_bag_.isOpen() && 
            (current_time - current_bag_start_time_) >= max_bag_duration_) {
            finalizeCurrentBagSegment(current_time);
            startNewBagSegment(current_time);
        }
        
        messages_recorded_++;
    }
    
    // 确保最后的bag段被正确关闭
    if (current_bag_.isOpen()) {
        finalizeCurrentBagSegment(ros::Time::now().toSec());
    }
    
    ROS_INFO("Recording worker stopped");
}

void ConcurrentRecorder::processRecordingMessage(const MessageVariant& message)
{
    if (!is_recording_) {
        // 开始录制
        is_recording_ = true;
        double timestamp = ros::Time::now().toSec();
        startNewBagSegment(timestamp);
    }
    
    // 写入bag包
    if (current_bag_.isOpen()) {
        try {
            std::visit([this](const auto& msg) {
                using T = std::decay_t<decltype(msg)>;
                if constexpr (std::is_same_v<T, ImageMessage>) {
                    current_bag_.write(msg.topic, msg.msg.header.stamp, msg.msg);
                } else if constexpr (std::is_same_v<T, PointCloudMessage>) {
                    current_bag_.write(msg.topic, msg.msg.header.stamp, msg.msg);
                }
            }, message);
        } catch (rosbag::BagException& e) {
            ROS_ERROR("Failed to write to bag: %s", e.what());
        }
    }
}

void ConcurrentRecorder::dbMaintainWorker()
{
    ROS_INFO("Database maintenance worker started");
    
    while (!stop_threads_ && !g_shutdown_requested && ros::ok())
    {
        // 每隔cleanup_interval_秒执行一次清理
        std::this_thread::sleep_for(std::chrono::seconds(cleanup_interval_));
        
        // 计算3天前的时间戳
        double cutoff_time = ros::Time::now().toSec() - (max_retention_days_ * 24 * 3600);
        
        // 查询需要删除的bag文件路径
        std::vector<std::string> old_bag_paths;
        const char* select_sql = "SELECT bag_path FROM bag_segments WHERE end_time < ?";
        sqlite3_stmt* stmt;
        
        if (sqlite3_prepare_v2(db_, select_sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_double(stmt, 1, cutoff_time);
            
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char* bag_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                old_bag_paths.push_back(bag_path);
            }
            
            sqlite3_finalize(stmt);
        }
        
        // 删除对应的本地bag文件
        int deleted_files = 0;
        for (const auto& bag_path : old_bag_paths) {
            try {
                if (fs::remove(bag_path)) {
                    deleted_files++;
                    ROS_INFO("Deleted old bag file: %s", bag_path.c_str());
                }
            } catch (const fs::filesystem_error& e) {
                ROS_WARN("Failed to delete bag file %s: %s", bag_path.c_str(), e.what());
            }
        }
        
        // 删除数据库记录
        const char* delete_sql = "DELETE FROM bag_segments WHERE end_time < ?";
        if (sqlite3_prepare_v2(db_, delete_sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_double(stmt, 1, cutoff_time);
            
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                ROS_ERROR("Failed to delete old bag records: %s", sqlite3_errmsg(db_));
            }
            
            sqlite3_finalize(stmt);
        }
        
        ROS_INFO("Database maintenance completed. Deleted %d bag files and corresponding records.", deleted_files);
    }
}

void ConcurrentRecorder::initDatabase() {
    if (sqlite3_open(db_path_.c_str(), &db_) == SQLITE_OK) {
        // 创建bag段表
        const char* create_bag_table = 
            "CREATE TABLE IF NOT EXISTS bag_segments ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "bag_path TEXT NOT NULL UNIQUE,"
            "start_time REAL NOT NULL,"
            "end_time REAL NOT NULL,"
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
            ");";
        
        char* err_msg = nullptr;
        if (sqlite3_exec(db_, create_bag_table, nullptr, nullptr, &err_msg) != SQLITE_OK) {
            ROS_ERROR("SQL error: %s", err_msg);
            sqlite3_free(err_msg);
        }
        
        // 创建索引以提高查询性能
        const char* create_index = 
            "CREATE INDEX IF NOT EXISTS idx_bag_time_range ON bag_segments (start_time, end_time)";
        
        if (sqlite3_exec(db_, create_index, nullptr, nullptr, &err_msg) != SQLITE_OK) {
            ROS_ERROR("SQL error: %s", err_msg);
            sqlite3_free(err_msg);
        }
        
        ROS_INFO("Database initialized successfully");
    } else {
        ROS_ERROR("Failed to open database: %s", sqlite3_errmsg(db_));
    }
}

void ConcurrentRecorder::loadConfig(const std::string& config_file) {
    try {
        if (fs::exists(config_file)) {
            YAML::Node config = YAML::LoadFile(config_file);
            
            // 图像话题配置
            if (config["image_topic"]) {
                image_topics_ = config["image_topic"].as<std::vector<std::string>>();
            }

            // 点云话题配置
            if (config["lidar_topic"]) {
                point_cloud_topics_ = config["lidar_topic"].as<std::vector<std::string>>();
            }
            
            // 录制性能配置
            if (config["max_bag_duration"]) {
                max_bag_duration_ = config["max_bag_duration"].as<double>();
            }
            
            // 数据库清理配置
            if (config["cleanup_interval"]) {
                cleanup_interval_ = config["cleanup_interval"].as<int>();
            }
            
            if (config["max_retention_days"]) {
                max_retention_days_ = config["max_retention_days"].as<int>();
            }
            
            ROS_INFO("Configuration loaded from: %s", config_file.c_str());
            ROS_INFO("Max Bag Duration: %.1f seconds", max_bag_duration_);
            ROS_INFO("Cleanup Interval: %d seconds, Max Retention: %d days", 
                     cleanup_interval_, max_retention_days_);
            
            // 打印订阅的话题
            for (const auto& topic : image_topics_) {
                ROS_INFO("Image topic: %s", topic.c_str());
            }
            for (const auto& topic : point_cloud_topics_) {
                ROS_INFO("Point cloud topic: %s", topic.c_str());
            }
        } else {
            throw std::runtime_error("Config file not found");
        }
    } catch (const std::exception& e) {
        ROS_WARN("Failed to load config: %s. Using default settings.", e.what());
        
        // 默认配置
        max_bag_duration_ = 1800.0; // 30分钟
        cleanup_interval_ = 28800; // 8小时
        max_retention_days_ = 3; // 3天
        
        // 默认话题
        image_topics_ = {"/image_rect/compressed", "/image_rect"};
        point_cloud_topics_ = {"/lidar/livox_cloud1", "/lidar/livox_cloud2", "/lidar/livox_cloud3"};
    }
}