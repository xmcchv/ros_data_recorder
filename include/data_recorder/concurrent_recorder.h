#ifndef CONCURRENT_RECORDER_H
#define CONCURRENT_RECORDER_H

#include <ros/ros.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <std_msgs/Float64MultiArray.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <sensor_msgs/image_encodings.h>
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
#include <sqlite3.h>
#include <yaml-cpp/yaml.h>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <filesystem>
#include <chrono>
#include <ctime>
#include <csignal>
#include <atomic>
#include <memory>
#include <map>
#include <vector>

namespace fs = std::filesystem;

class ConcurrentRecorder
{
public:
    ConcurrentRecorder();
    ~ConcurrentRecorder();
    
    void run();
    void stop();

private:
    // 配置管理
    void loadConfig(const std::string& config_file);
    
    // 数据库操作
    sqlite3* db_;
    void initDatabase();

    // 回调函数
    void timestampCallback(const std_msgs::Float64MultiArray::ConstPtr& msg);
    void imageCallback(const sensor_msgs::CompressedImage::ConstPtr& msg);
    
    // 工作线程函数
    void recordingWorker();
    void playbackWorker();
    
    // 消息处理
    void processRecordingMessage(const sensor_msgs::CompressedImage& msg);
    void playbackFromBag(double start_timestamp, double end_timestamp);
    
    // ROS相关
    ros::NodeHandle nh_;
    ros::Subscriber timestamp_sub_;
    ros::Subscriber image_sub_;
    ros::Publisher image_pub_;
    
    // 配置和路径
    std::string base_dir_;
    std::string db_path_;
    std::map<std::string, std::string> config_;
    
    // 线程管理
    std::thread recording_thread_;
    std::thread playback_thread_;
    std::atomic<bool> stop_threads_{false};
    std::atomic<bool> is_recording_{true};
    
    // 数据队列和同步
    std::queue<std::pair<double, double>> timestamp_queue_;
    std::queue<sensor_msgs::CompressedImage> image_queue_;
    std::mutex timestamp_mutex_;
    std::mutex image_mutex_;
    std::condition_variable image_cv_;
    
    // 性能统计
    std::atomic<uint64_t> messages_recorded_{0};
    std::atomic<uint64_t> messages_played_{0};
};

#endif // CONCURRENT_RECORDER_H