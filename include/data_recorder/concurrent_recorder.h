#ifndef CONCURRENT_RECORDER_H
#define CONCURRENT_RECORDER_H

#include <ros/ros.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <sensor_msgs/PointCloud2.h>
#include <std_msgs/Float64MultiArray.h>
#include <rosbag/bag.h>
#include <sqlite3.h>
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <filesystem>
#include <chrono>
#include <ctime>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <vector>
#include <variant>

namespace fs = std::filesystem;

// 消息类型定义
struct ImageMessage {
    sensor_msgs::CompressedImage msg;
    std::string topic;
};

struct PointCloudMessage {
    sensor_msgs::PointCloud2 msg;
    std::string topic;
};

using MessageVariant = std::variant<ImageMessage, PointCloudMessage>;

class ConcurrentRecorder {
public:
    ConcurrentRecorder();
    ~ConcurrentRecorder();
    
    void stop();
    void run();

private:
    void loadConfig(const std::string& config_file);
    void initDatabase();
    void imageCallback(const sensor_msgs::CompressedImage::ConstPtr& msg, const std::string& topic);
    void pointCloudCallback(const sensor_msgs::PointCloud2::ConstPtr& msg, const std::string& topic);
    void recordingWorker();
    void dbMaintainWorker();
    void processRecordingMessage(const MessageVariant& message);
    void startNewBagSegment(double start_timestamp);
    void finalizeCurrentBagSegment(double end_timestamp);
    
    ros::NodeHandle nh_;
    std::vector<ros::Subscriber> image_subs_;
    std::vector<ros::Subscriber> point_cloud_subs_;
    
    std::thread recording_thread_;
    std::thread db_maintain_thread_;
    
    std::queue<MessageVariant> message_queue_;
    std::mutex message_mutex_;
    std::condition_variable message_cv_;
    
    sqlite3* db_;
    std::string base_dir_;
    std::string db_path_;
    
    std::atomic<bool> stop_threads_{false};
    std::atomic<bool> is_recording_{false};
    std::atomic<size_t> messages_recorded_{0};
    
    // bag包录制相关变量
    rosbag::Bag current_bag_;
    std::string current_bag_path_;
    double current_bag_start_time_;
    double max_bag_duration_; // 最大bag包时长（秒）
    int cleanup_interval_; // 清理间隔（秒）
    int max_retention_days_; // 最大保留天数
    
    // 话题录制列表
    std::vector<std::string> image_topics_;
    std::vector<std::string> point_cloud_topics_;
};

#endif // CONCURRENT_RECORDER_H