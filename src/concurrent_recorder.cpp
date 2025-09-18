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

ConcurrentRecorder::ConcurrentRecorder() : nh_("~"), max_bag_duration_(3600.0)
{
    // 加载配置
    std::string config_file;
    nh_.param("config_file", config_file, std::string("config/recorder_config.yaml"));
    loadConfig(config_file);
    
    // 从配置读取持续时间（如果存在）
    if (config_.find("max_bag_duration") != config_.end()) {
        try {
            max_bag_duration_ = std::stod(config_["max_bag_duration"]);
            if (max_bag_duration_ > 0) {
                max_bag_duration_ = std::floor(max_bag_duration_ / 60) * 60;
                max_bag_duration_ = std::max(max_bag_duration_, 60.0);  // 最小1分钟
            }
            ROS_INFO("Loaded bag duration: %.1f seconds", max_bag_duration_);
        } catch (...) {
            ROS_WARN("Invalid max_bag_duration config, using default 3600s");
        }
    }
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
    management_thread_ = std::thread(&ConcurrentRecorder::bagManagementWorker, this);
    
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
    if (management_thread_.joinable()) management_thread_.join();
    
    closeCurrentBag();
    
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

void ConcurrentRecorder::bagManagementWorker()
{
    ROS_INFO("Bag management worker started");
    
    while (!stop_threads_ && !g_shutdown_requested && ros::ok())
    {
        // 每小时检查是否需要创建新的bag文件
        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_time);
        
        // 如果当前分钟是0，表示整点，检查是否需要创建新文件
        if (now_tm->tm_min == 0)
        {
            std::lock_guard<std::mutex> lock(bag_mutex_);
            if (!current_bag_ || !current_bag_->isOpen())
            {
                createNewBag();
            }
            else
            {
                // 检查当前bag文件是否已经超过配置的最大时长
                double current_duration = (ros::Time::now() - bag_start_time_).toSec();
                if (current_duration >= max_bag_duration_)
                {
                    closeCurrentBag();
                    createNewBag();
                }
            }
        }
        
        std::this_thread::sleep_for(std::chrono::minutes(1)); // 每分钟检查一次
    }
}

void ConcurrentRecorder::processRecordingMessage(const sensor_msgs::CompressedImage& msg)
{
    std::lock_guard<std::mutex> lock(bag_mutex_);
    
    if (!current_bag_ || !current_bag_->isOpen())
    {
        createNewBag();
    }
    
    try
    {
        ros::Time stamp = msg.header.stamp.sec == 0 ? ros::Time::now() : msg.header.stamp;
        current_bag_->write("/detectImage/compressed", stamp, msg);
    }
    catch (const std::exception& e)
    {
        ROS_ERROR("Error writing to bag: %s", e.what());
    }
}

void ConcurrentRecorder::playbackFromBag(double start_timestamp, double end_timestamp)
{
    ROS_INFO("Playing back from %.6f to %.6f", start_timestamp, end_timestamp);
    
    auto bag_files = findBagFiles(start_timestamp, end_timestamp);
    if (bag_files.empty())
    {
        ROS_WARN("No bag files found for the specified time range");
        return;
    }
    
    for (const auto& bag_file : bag_files)
    {
        if (stop_threads_ || !ros::ok()) break;
        
        try
        {
            rosbag::Bag bag(bag_file, rosbag::bagmode::Read);
            
            rosbag::View full_view(bag);
            ros::Time bag_start_time = full_view.getBeginTime();
            ros::Time bag_end_time = full_view.getEndTime();
            
            double bag_start = bag_start_time.toSec();
            double bag_end = bag_end_time.toSec();
            
            double play_start = std::max(start_timestamp, bag_start);
            double play_end = std::min(end_timestamp, bag_end);
            
            if (play_start >= play_end) 
            {
                ROS_INFO("Skipping bag %s - no overlap with requested time range", bag_file.c_str());
                bag.close();
                continue;
            }
            
            ROS_INFO("Playing %s from %.6f to %.6f", bag_file.c_str(), play_start, play_end);
            
            ros::Time start_time = ros::Time().fromSec(play_start);
            ros::Time end_time = ros::Time().fromSec(play_end);
            
            rosbag::View view(bag, rosbag::TopicQuery("/detectImage/compressed"), start_time, end_time);
            
            for (const auto& m : view)
            {
                if (stop_threads_ || !ros::ok()) break;
                
                auto compressed_msg = m.instantiate<sensor_msgs::CompressedImage>();
                if (compressed_msg)
                {
                    try
                    {
                        cv::Mat cv_image = cv::imdecode(cv::Mat(compressed_msg->data), cv::IMREAD_COLOR);
                        if (!cv_image.empty())
                        {
                            cv_bridge::CvImage cv_bridge_img;
                            cv_bridge_img.header = compressed_msg->header;
                            cv_bridge_img.encoding = sensor_msgs::image_encodings::BGR8;
                            cv_bridge_img.image = cv_image;
                            
                            image_pub_.publish(cv_bridge_img.toImageMsg());
                            messages_played_++;
                        }
                    }
                    catch (const std::exception& e)
                    {
                        ROS_ERROR("Error processing image: %s", e.what());
                    }
                }
                
                std::this_thread::sleep_for(std::chrono::milliseconds(33)); // ~30fps
            }
            
            bag.close();
        }
        catch (const std::exception& e)
        {
            ROS_ERROR("Error playing bag %s: %s", bag_file.c_str(), e.what());
        }
    }
}

std::vector<std::string> ConcurrentRecorder::findBagFiles(double start_ts, double end_ts)
{
    std::vector<std::string> result;
    sqlite3* db;
    
    if (sqlite3_open(db_path_.c_str(), &db) != SQLITE_OK)
    {
        ROS_ERROR("Cannot open database: %s", sqlite3_errmsg(db));
        return result;
    }
    
    const char* sql = "SELECT filename FROM recordings "
                     "WHERE start_time <= ? AND end_time >= ? "
                     "ORDER BY start_time";
    
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK)
    {
        sqlite3_bind_double(stmt, 1, end_ts);
        sqlite3_bind_double(stmt, 2, start_ts);
        
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            const char* filename = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            std::string filepath = base_dir_ + "/" + filename;
            if (fs::exists(filepath))
            {
                result.push_back(filepath);
                ROS_INFO("Found matching bag file: %s", filepath.c_str());
            }
            else
            {
                ROS_WARN("Bag file not found: %s", filepath.c_str());
            }
        }
    }
    else
    {
        ROS_ERROR("Failed to prepare SQL statement: %s", sqlite3_errmsg(db));
    }
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return result;
}

void ConcurrentRecorder::createNewBag()
{
    closeCurrentBag();
    
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm now_tm = *std::localtime(&now_time);
    
    // 计算结束时间
    std::time_t end_time = now_time + static_cast<time_t>(max_bag_duration_);
    std::tm end_tm = *std::localtime(&end_time);
    
    char time_str[20];
    std::strftime(time_str, sizeof(time_str), "%Y%m%d_%H%M", &now_tm);
    
    char end_str[10];
    std::strftime(end_str, sizeof(end_str), "_%H%M", &end_tm);
    
    std::string filename = "recording_" + std::string(time_str) + end_str + ".bag";
    std::string filepath = base_dir_ + "/" + filename;
    
    bool is_new_file = !fs::exists(filepath);
    
    try
    {
        if (is_new_file) 
        {
            current_bag_ = std::make_unique<rosbag::Bag>(filepath, rosbag::bagmode::Write);
            current_bag_->close();  
        }
        current_bag_ = std::make_unique<rosbag::Bag>(filepath, rosbag::bagmode::Append);
        
        bag_start_time_ = ros::Time::now();
        
        // 计算预计结束时间
        double expected_end_time = bag_start_time_.toSec() + max_bag_duration_;
        addRecordingToDB(filename, bag_start_time_.toSec(), expected_end_time, 0);

        if(is_new_file)
        {
            ROS_INFO("Created new bag file: %s", filepath.c_str());
        }
        else
        {
            ROS_INFO("Appended to bag file: %s", filepath.c_str());
        }
    }
    catch (const std::exception& e)
    {
        ROS_ERROR("Error creating bag file: %s", e.what());
        current_bag_.reset();
    }
}

void ConcurrentRecorder::closeCurrentBag()
{
    if (current_bag_ && current_bag_->isOpen())
    {
        try
        {
            std::string filename = fs::path(current_bag_->getFileName()).filename().string();
            uint64_t file_size = fs::file_size(current_bag_->getFileName());
            double bag_start = bag_start_time_.toSec();
            double bag_end = ros::Time::now().toSec();
            
            addRecordingToDB(filename, bag_start, bag_end, file_size);
            current_bag_->close();
            
            ROS_INFO("Closed bag file: %s, size: %lu bytes", filename.c_str(), file_size);
        }
        catch (const std::exception& e)
        {
            ROS_ERROR("Error closing bag: %s", e.what());
        }
    }
}

void ConcurrentRecorder::initDatabase()
{
    sqlite3* db;
    if (sqlite3_open(db_path_.c_str(), &db) == SQLITE_OK)
    {
        const char* sql = "CREATE TABLE IF NOT EXISTS recordings ("
                       "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                       "filename TEXT NOT NULL UNIQUE,"
                       "start_time REAL NOT NULL,"
                       "end_time REAL NOT NULL,"
                       "duration REAL,"
                       "file_size INTEGER,"
                       "created_at TEXT DEFAULT CURRENT_TIMESTAMP)";
        
        char* err_msg = nullptr;
        if (sqlite3_exec(db, sql, nullptr, nullptr, &err_msg) != SQLITE_OK)
        {
            ROS_ERROR("SQL error: %s", err_msg);
            sqlite3_free(err_msg);
        }
        sqlite3_close(db);
    }
}

void ConcurrentRecorder::addRecordingToDB(const std::string& filename, double start_time, double end_time, uint64_t file_size)
{
    sqlite3* db;
    if (sqlite3_open(db_path_.c_str(), &db) == SQLITE_OK)
    {
        const char* sql = "INSERT OR REPLACE INTO recordings (filename, start_time, end_time, duration, file_size) "
                          "VALUES (?, ?, ?, ?, ?)";
        
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK)
        {
            sqlite3_bind_text(stmt, 1, filename.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_double(stmt, 2, start_time);
            sqlite3_bind_double(stmt, 3, end_time);
            sqlite3_bind_double(stmt, 4, end_time - start_time);
            sqlite3_bind_int64(stmt, 5, file_size);
            
            if (sqlite3_step(stmt) != SQLITE_DONE)
            {
                ROS_ERROR("Failed to update recording: %s", sqlite3_errmsg(db));
            }
        }
        sqlite3_finalize(stmt);
        sqlite3_close(db);
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