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

ConcurrentRecorder::ConcurrentRecorder() : nh_("~"), db_(nullptr)
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
    
    // 设置视频参数
    fps_ = 30.0; // 默认帧率
    segment_duration_ = 300; // 默认5分钟分段
    
    // 启动工作线程
    recording_thread_ = std::thread(&ConcurrentRecorder::recordingWorker, this);
    playback_thread_ = std::thread(&ConcurrentRecorder::playbackWorker, this);
    db_maintain_thread_ = std::thread(&ConcurrentRecorder::dbMaintainWorker, this);
    
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
    if (db_maintain_thread_.joinable()) db_maintain_thread_.join();

    // 确保当前视频段被正确关闭
    if (video_writer_.isOpened()) {
        finalizeCurrentVideoSegment(ros::Time::now().toSec());
    }

    if (db_) {
        sqlite3_close(db_);
    }
    
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
    if (!is_recording_) {
        // 开始录制
        is_recording_ = true;
        double timestamp = msg->header.stamp.toSec();
        startNewVideoSegment(timestamp);
    }
    
    std::lock_guard<std::mutex> lock(image_mutex_);
    image_queue_.push(*msg);
    image_cv_.notify_one();
}

void ConcurrentRecorder::startNewVideoSegment(double start_timestamp)
{
    // 生成视频文件名
    std::time_t t = static_cast<std::time_t>(start_timestamp);
    std::tm* tm = std::localtime(&t);
    char filename[100];
    std::strftime(filename, sizeof(filename), "%Y%m%d_%H%M%S", tm);
    
    current_video_path_ = base_dir_ + "/" + std::string(filename) + ".mp4";
    current_segment_start_time_ = start_timestamp;
    current_segment_frame_count_ = 0;
    
    ROS_INFO("Starting new video segment: %s", current_video_path_.c_str());
}

void ConcurrentRecorder::finalizeCurrentVideoSegment(double end_timestamp)
{
    if (video_writer_.isOpened()) {
        video_writer_.release();
        
        // 记录到数据库
        sqlite3_stmt* stmt;
        const char* sql = "INSERT INTO video_segments (video_path, start_time, end_time, frame_count) VALUES (?, ?, ?, ?)";
        
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, current_video_path_.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_double(stmt, 2, current_segment_start_time_);
            sqlite3_bind_double(stmt, 3, end_timestamp);
            sqlite3_bind_int(stmt, 4, current_segment_frame_count_);
            
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                ROS_ERROR("Failed to insert video segment into database: %s", sqlite3_errmsg(db_));
            }
            
            sqlite3_finalize(stmt);
        }
        
        ROS_INFO("Finalized video segment: %s (frames: %d)", current_video_path_.c_str(), current_segment_frame_count_);
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
    
    // 确保最后一段视频被正确关闭
    if (video_writer_.isOpened()) {
        finalizeCurrentVideoSegment(ros::Time::now().toSec());
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
        
        if (image.empty()) {
            ROS_WARN("Failed to decode image");
            return;
        }
        
        double timestamp = msg.header.stamp.toSec();
        
        // 检查是否需要创建新的视频段
        if (!video_writer_.isOpened() || 
            (timestamp - current_segment_start_time_ > segment_duration_)) {
            
            if (video_writer_.isOpened()) {
                finalizeCurrentVideoSegment(timestamp);
            }
            
            startNewVideoSegment(timestamp);
            
            // 初始化视频写入器
            frame_size_ = image.size();
            int fourcc = cv::VideoWriter::fourcc('H', '2', '6', '4'); // H.264编码
            video_writer_.open(current_video_path_, fourcc, fps_, frame_size_, true);
            
            if (!video_writer_.isOpened()) {
                ROS_ERROR("Failed to open video writer for path: %s", current_video_path_.c_str());
                return;
            }
        }
        
        // 写入视频帧
        video_writer_.write(image);
        current_segment_frame_count_++;
        current_segment_end_time_ = timestamp;
        
    } catch (const std::exception& e) {
        ROS_ERROR("Process image failed: %s", e.what());
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
                ROS_WARN("Detect new timestamp request, interrupt current playback");
                interrupt_flag = true;
                break;
            }
        }
    });

    // 从数据库查询符合条件的视频段
    std::vector<std::tuple<std::string, double, double>> video_segments;
    sqlite3_stmt* stmt;
    const char* query = "SELECT video_path, start_time, end_time FROM video_segments "
                        "WHERE start_time <= ? AND end_time >= ? ORDER BY start_time ASC";

    if (sqlite3_prepare_v2(db_, query, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_double(stmt, 1, end_timestamp);
        sqlite3_bind_double(stmt, 2, start_timestamp);

        while (sqlite3_step(stmt) == SQLITE_ROW && !stop_threads_ && ros::ok()) {
            if (interrupt_flag) {
                ROS_INFO("Current playback interrupted");
                break;
            }

            const char* video_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            double seg_start = sqlite3_column_double(stmt, 1);
            double seg_end = sqlite3_column_double(stmt, 2);
            
            video_segments.emplace_back(video_path, seg_start, seg_end);
        }
        sqlite3_finalize(stmt);
    }

    ROS_INFO("Found %lu video segments for time range %.6f to %.6f", 
             video_segments.size(), start_timestamp, end_timestamp);

    // 处理每个视频段
    size_t published_count = 0;
    for (const auto& segment : video_segments) {
        if (interrupt_flag || stop_threads_ || !ros::ok()) {
            ROS_INFO("Playback interrupted, published %lu frames", published_count);
            break;
        }

        std::string video_path = std::get<0>(segment);
        double seg_start = std::get<1>(segment);
        double seg_end = std::get<2>(segment);
        
        // 打开视频文件
        cv::VideoCapture cap(video_path);
        if (!cap.isOpened()) {
            ROS_WARN("Failed to open video: %s", video_path.c_str());
            continue;
        }
        
        // 计算需要提取的帧范围
        double video_fps = cap.get(cv::CAP_PROP_FPS);
        if (video_fps <= 0) {
            ROS_WARN("Invalid FPS for video: %s", video_path.c_str());
            cap.release();
            continue;
        }

        // 计算起始和结束帧
        int start_frame = 0;
        int total_frames = static_cast<int>(cap.get(cv::CAP_PROP_FRAME_COUNT));
        int end_frame = total_frames - 1;

        // 确保视频段确实包含请求的时间范围
        if (start_timestamp > seg_end || end_timestamp < seg_start) {
            ROS_WARN("Video segment doesn't contain requested time range: %s", video_path.c_str());
            cap.release();
            continue;
        }

        // 计算起始帧
        if (start_timestamp > seg_start) {
            double offset = start_timestamp - seg_start;
            start_frame = static_cast<int>(std::floor(offset * video_fps));
        }

        // 计算结束帧
        if (end_timestamp < seg_end) {
            double offset = end_timestamp - seg_start;
            end_frame = static_cast<int>(std::ceil(offset * video_fps));
        } else {
            end_frame = total_frames - 1;
        }

        // 增加时间范围校验
        if (end_timestamp - start_timestamp < 1.0/video_fps) {
            end_frame = start_frame + 1;
        }

        // 确保帧号在有效范围内
        start_frame = std::clamp(start_frame, 0, total_frames-1);
        end_frame = std::clamp(end_frame, start_frame, total_frames-1);

        ROS_INFO("Playing video segment: %s, start_frame: %d, end_frame: %d start_timestamp: %.6f end_timestamp: %.6f seg_start: %.6f seg_end: %.6f", 
                        video_path.c_str(), start_frame, end_frame, start_timestamp, end_timestamp, seg_start, seg_end);
        // 检查有效性
        if (start_frame > end_frame) {
            ROS_WARN("Invalid frame range: start_frame(%d) > end_frame(%d) for video: %s", 
                    start_frame, end_frame, video_path.c_str());
            ROS_WARN("Requested time range: %.6f to %.6f", start_timestamp, end_timestamp);
            ROS_WARN("Video segment time range: %.6f to %.6f", seg_start, seg_end);
            cap.release();
            continue;
        }

        
        // 设置起始帧
        if (!cap.set(cv::CAP_PROP_POS_FRAMES, start_frame)) {
            ROS_WARN("Can't locate start frame: %d, trying to seek to beginning", start_frame);
            // 如果设置特定帧失败，尝试重置到开头
            cap.set(cv::CAP_PROP_POS_FRAMES, 0);
            // 跳过前面的帧
            for (int i = 0; i < start_frame; i++) {
                cv::Mat dummy;
                if (!cap.read(dummy)) {
                    ROS_ERROR("Failed to skip frame %d", i);
                    break;
                }
            }
        }
        
        // 读取并发布帧
        cv::Mat frame;
        int current_frame = start_frame;
        
        while (current_frame <= end_frame) {
            if (interrupt_flag || stop_threads_ || !ros::ok()) {
                break;
            }
            
            // 读取帧
            if (!cap.read(frame) || frame.empty()) {
                ROS_ERROR("Failed to read frame %d/%d", current_frame, end_frame);
                break;
            }

            // 计算当前帧的时间戳
            double frame_time = seg_start + (current_frame / video_fps);
            ROS_INFO("Publishing frame %d at timestamp %.6f", current_frame, frame_time);
            // 发布图像
            cv_bridge::CvImage cv_image;
            cv_image.image = frame;
            cv_image.encoding = "bgr8";
            cv_image.header.frame_id = "map";
            cv_image.header.stamp = ros::Time().fromSec(frame_time);
            image_pub_.publish(cv_image.toImageMsg());
            
            published_count++;
            messages_played_++;
            
            // 控制播放速度
            ros::Duration(1.0 / video_fps).sleep();
            current_frame++;
        }
        
        cap.release();
    }

    ROS_INFO("Playback finished, published %lu frames", published_count);

    interrupt_flag = true;
    if (interrupt_checker.joinable()) {
        interrupt_checker.join();
    }
}

void ConcurrentRecorder::dbMaintainWorker()
{
    ROS_INFO("Database maintain worker started");
    
    while (!stop_threads_ && !g_shutdown_requested && ros::ok())
    {
        std::this_thread::sleep_for(std::chrono::seconds(600));
        // 查询旧视频记录，时间超过当前时间-max_video_duration_的视频段
        const char* select_old_segments = 
            "SELECT id, video_path, start_time, end_time FROM video_segments "
            "WHERE end_time < ?";
        // 执行查询
        std::vector<std::tuple<std::string, double, double>> old_segments;
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db_, select_old_segments, -1, &stmt, nullptr) == SQLITE_OK) {
            double threshold = ros::Time::now().toSec() - max_video_duration_;
            sqlite3_bind_double(stmt, 1, threshold);
            
            
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                std::string video_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                double start_time = sqlite3_column_double(stmt, 2);
                double end_time = sqlite3_column_double(stmt, 3);
                old_segments.emplace_back(video_path, start_time, end_time);
            }
            sqlite3_finalize(stmt);
        }

        // 根据视频路径删除旧的视频文件
        for (const auto& [video_path, start_time, end_time] : old_segments) {
            if (fs::exists(video_path)) {
                fs::remove(video_path);
                ROS_INFO("Deleted old video file: %s", video_path.c_str());
            }
        }

        // 清理旧的视频段记录
        const char* delete_old_segments = 
            "DELETE FROM video_segments WHERE end_time < ?";
        // sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db_, delete_old_segments, -1, &stmt, nullptr) == SQLITE_OK) {
            double threshold = ros::Time::now().toSec() - max_video_duration_;
            sqlite3_bind_double(stmt, 1, threshold);
            
            if (sqlite3_step(stmt) == SQLITE_DONE) {
                ROS_INFO("Cleaned up %d old video segments", sqlite3_changes(db_));
            } else {
                ROS_ERROR("Failed to clean up old video segments: %s", sqlite3_errmsg(db_));
            }
            
            sqlite3_finalize(stmt);
        }


    }

}

void ConcurrentRecorder::initDatabase() {
    if (sqlite3_open(db_path_.c_str(), &db_) == SQLITE_OK) {
        // 创建视频段表
        const char* create_video_table = 
            "CREATE TABLE IF NOT EXISTS video_segments ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "video_path TEXT NOT NULL UNIQUE,"
            "start_time REAL NOT NULL,"
            "end_time REAL NOT NULL,"
            "frame_count INTEGER NOT NULL,"
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP"
            ");";
        
        char* err_msg = nullptr;
        if (sqlite3_exec(db_, create_video_table, nullptr, nullptr, &err_msg) != SQLITE_OK) {
            ROS_ERROR("SQL error: %s", err_msg);
            sqlite3_free(err_msg);
        }
        
        // 创建索引以提高查询性能
        const char* create_index = 
            "CREATE INDEX IF NOT EXISTS idx_time_range ON video_segments (start_time, end_time)";
        
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
            
            if (config["fps"]) {
                fps_ = config["fps"].as<double>();
            }
            
            if (config["segment_duration"]) {
                segment_duration_ = config["segment_duration"].as<int>();
            }
            
            if (config["compression_quality"]) {
                config_["compression_quality"] = std::to_string(config["compression_quality"].as<int>());
            }
            
            if (config["image_compression_format"]) {
                config_["image_compression_format"] = config["image_compression_format"].as<std::string>();
            }

            if (config["max_video_duration"]) {
                max_video_duration_ = config["max_video_duration"].as<int>();
            }
            
            ROS_INFO("Configuration loaded from: %s", config_file.c_str());
            ROS_INFO("FPS: %.2f, Segment Duration: %d seconds, Max Video Duration: %d seconds", fps_, segment_duration_, max_video_duration_);
        } else {
            throw std::runtime_error("Config file not found");
        }
    } catch (const std::exception& e) {
        ROS_WARN("Failed to load config: %s. Using default settings.", e.what());
        fps_ = 30.0;
        segment_duration_ = 300; // 5分钟
        config_["compression_quality"] = "90";
        config_["image_compression_format"] = "jpeg";
    }
}