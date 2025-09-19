#include "data_recorder/concurrent_recorder.h"
#include <ros/ros.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <std_msgs/Float64MultiArray.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
#include <sqlite3.h>
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <filesystem>
#include <chrono>
#include <ctime>

namespace fs = std::filesystem;

// 全局关闭标志
std::atomic<bool> g_shutdown_requested{false};

void signalHandler(int signal)
{
    g_shutdown_requested = true;
    ros::shutdown();
}

ConcurrentRecorder::ConcurrentRecorder() : nh_("~")
{
    // 加载配置
    std::string config_file;
    nh_.param("config_file", config_file, std::string("config/recorder_config.yaml"));
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
    
    // 设置发布器
    image_pub_ = nh_.advertise<sensor_msgs::Image>("/recordImage", 10);
    
    // 订阅时间戳控制消息
    timestamp_sub_ = nh_.subscribe("/recordTimes", 100, &ConcurrentRecorder::timestampCallback, this);
    
    // 订阅压缩图像话题
    std::string image_topic;
    nh_.param("image_topic", image_topic, std::string("/detectImage/compressed"));
    image_sub_ = nh_.subscribe(image_topic, 100, &ConcurrentRecorder::imageCallback, this);
    
    // 启动工作线程
    recording_thread_ = std::thread(&ConcurrentRecorder::recordingWorker, this);
    playback_thread_ = std::thread(&ConcurrentRecorder::playbackWorker, this);
    
    ROS_INFO("Concurrent recorder initialized with data directory: %s", base_dir_.c_str());
}

ConcurrentRecorder::~ConcurrentRecorder()
{
    stop();
}

void ConcurrentRecorder::stop()
{
    stop_threads_ = true;
    image_cv_.notify_all();
    
    if (recording_thread_.joinable()) recording_thread_.join();
    if (playback_thread_.joinable()) playback_thread_.join();

    sqlite3_close(db_);
    
    ROS_INFO("Recorder stopped. Recorded %lu messages, played %lu messages.", 
             messages_recorded_.load(), messages_played_.load());
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

void ConcurrentRecorder::timestampCallback(const std_msgs::Float64MultiArray::ConstPtr& msg)
{
    if (msg->data.size() >= 2)
    {
        double start_timestamp = msg->data[0];
        double end_timestamp = msg->data[1];
        
        if (start_timestamp > 0 && end_timestamp > start_timestamp)
        {
            std::lock_guard<std::mutex> lock(timestamp_mutex_);
            timestamp_queue_.push({start_timestamp, end_timestamp});
            ROS_INFO("Received playback request: %.6f to %.6f", start_timestamp, end_timestamp);
        }
        else
        {
            ROS_WARN("Invalid timestamp range: %.6f to %.6f", start_timestamp, end_timestamp);
        }
    }
    else
    {
        ROS_WARN("Invalid timestamp message format");
    }
}

void ConcurrentRecorder::imageCallback(const sensor_msgs::CompressedImage::ConstPtr& msg)
{
    if (is_recording_)
    {
        std::lock_guard<std::mutex> lock(image_mutex_);
        image_queue_.push(*msg);
        image_cv_.notify_one();
    }
}

void ConcurrentRecorder::recordingWorker()
{
    ROS_INFO("Recording worker started");
    
    while (!stop_threads_ && !g_shutdown_requested && ros::ok())
    {
        sensor_msgs::CompressedImage msg;
        {
            std::unique_lock<std::mutex> lock(image_mutex_);
            image_cv_.wait_for(lock, std::chrono::milliseconds(100), 
                [this] { return !image_queue_.empty() || stop_threads_; });
            
            if (stop_threads_) break;
            
            if (image_queue_.empty()) continue;
            
            msg = image_queue_.front();
            image_queue_.pop();
        }
        
        processRecordingMessage(msg);
        messages_recorded_++;
    }
}

void ConcurrentRecorder::playbackWorker()
{
    ROS_INFO("Playback worker started");
    
    while (!stop_threads_ && !g_shutdown_requested && ros::ok())
    {
        std::pair<double, double> timestamps;
        {
            std::unique_lock<std::mutex> lock(timestamp_mutex_);
            if (timestamp_queue_.empty())
            {
                lock.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            timestamps = timestamp_queue_.front();
            timestamp_queue_.pop();
        }
        
        playbackFromBag(timestamps.first, timestamps.second);
    }
}

void ConcurrentRecorder::processRecordingMessage(const sensor_msgs::CompressedImage& msg) {
    try {
        // 解码压缩图像
        cv::Mat image = cv::imdecode(cv::Mat(msg.data), cv::IMREAD_COLOR);

        // 获取当前时间戳（带小数）
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        double timestamp = std::chrono::duration<double>(duration).count();

        // 生成带时间戳的文件名（小数点改为下划线）
        std::string timestamp_str = std::to_string(timestamp);
        std::replace(timestamp_str.begin(), timestamp_str.end(), '.', '_');
        std::string filename = timestamp_str + ".jpg";
        std::string image_path = base_dir_ + "/" + filename;

        // 保存图片
        cv::imwrite(image_path, image);

        // 写入数据库
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db_, "INSERT INTO recordings (image_path, timestamp) VALUES (?, ?)", -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, image_path.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_double(stmt, 2, timestamp);  // 使用 bind_double 存储时间戳
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);

        // 发布原始图像
        cv_bridge::CvImage cv_image;
        cv_image.image = image;
        cv_image.encoding = "bgr8";
        image_pub_.publish(cv_image.toImageMsg());
    } catch (const std::exception& e) {
        ROS_ERROR("处理图像失败: %s", e.what());
    }
}

void ConcurrentRecorder::playbackFromBag(double start_timestamp, double end_timestamp) {
    std::atomic<bool> interrupt_flag{false};
    
    // 启动中断检查线程
    std::thread interrupt_checker([&]() {
        while (!interrupt_flag && !stop_threads_ && ros::ok()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            std::lock_guard<std::mutex> lock(timestamp_mutex_);
            if (!timestamp_queue_.empty()) {
                ROS_WARN("检测到新时间戳请求，中断当前回放");
                interrupt_flag = true;
                break;
            }
        }
    });

    sqlite3_stmt* stmt;
    const char* query = "SELECT image_path, timestamp FROM recordings "
                        "WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp ASC";

    if (sqlite3_prepare_v2(db_, query, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_double(stmt, 1, start_timestamp);  
        sqlite3_bind_double(stmt, 2, end_timestamp);    

        while (sqlite3_step(stmt) == SQLITE_ROW && !stop_threads_ && ros::ok()) {
            if (interrupt_flag) {
                ROS_INFO("回放已被中断");
                break;
            }

            const char* image_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            int64_t timestamp = sqlite3_column_int64(stmt, 1);

            try {
                cv::Mat image = cv::imread(image_path, cv::IMREAD_COLOR);
                if (!image.empty()) {
                    // 转换为ROS消息并发布
                    cv_bridge::CvImage cv_image;
                    cv_image.image = image;
                    cv_image.encoding = "bgr8";
                    cv_image.header.stamp = ros::Time().fromSec(timestamp);
                    image_pub_.publish(cv_image.toImageMsg());
                    messages_played_++;
                }
            } catch (const std::exception& e) {
                ROS_ERROR("加载图片失败: %s | 路径: %s", e.what(), image_path);
            }
        }
        sqlite3_finalize(stmt);
    }

    interrupt_flag = true;
    if (interrupt_checker.joinable()) {
        interrupt_checker.join();
    }
}


void ConcurrentRecorder::initDatabase() {
    if (sqlite3_open(db_path_.c_str(), &db_) == SQLITE_OK)
    {
        const char* create_table = 
            "CREATE TABLE IF NOT EXISTS recordings ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "image_path TEXT NOT NULL,"
            "timestamp REAL NOT NULL," 
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
            ");";
        
        char* err_msg = nullptr;
        if (sqlite3_exec(db_, create_table, nullptr, nullptr, &err_msg) != SQLITE_OK)
        {
            ROS_ERROR("SQL error: %s", err_msg);
            sqlite3_free(err_msg);
        }
    }
}


void ConcurrentRecorder::loadConfig(const std::string& config_file)
{
    try {
        if (fs::exists(config_file)) {
            YAML::Node config = YAML::LoadFile(config_file);
            if(config["max_bag_duration"]) {
                config_["max_bag_duration"] = std::to_string(config["max_bag_duration"].as<double>());
            }
            config_["compression_quality"] = std::to_string(config["compression_quality"].as<int>(90));
            config_["image_compression_format"] = config["image_compression_format"].as<std::string>("jpeg");
            ROS_INFO("Configuration loaded from: %s", config_file.c_str());
        }
        else
        {
            throw std::runtime_error("Config file not found");
        }
    }
    catch (const std::exception& e)
    {
        ROS_WARN("Failed to load config: %s. Using default settings.", e.what());
        config_["compression_quality"] = "90";
        config_["image_compression_format"] = "jpeg";
    }
}