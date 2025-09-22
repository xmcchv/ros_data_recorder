#ifndef CONCURRENT_RECORDER_H
#define CONCURRENT_RECORDER_H

#include <ros/ros.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <std_msgs/Float64MultiArray.h>
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
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

namespace fs = std::filesystem;

class ConcurrentRecorder {
public:
    ConcurrentRecorder();
    ~ConcurrentRecorder();
    
    void stop();
    void run();

private:
    void loadConfig(const std::string& config_file);
    void initDatabase();
    void timestampCallback(const std_msgs::Float64MultiArray::ConstPtr& msg);
    void imageCallback(const sensor_msgs::CompressedImage::ConstPtr& msg);
    void recordingWorker();
    void playbackWorker();
    void processRecordingMessage(const sensor_msgs::CompressedImage& msg);
    void playbackFromBag(double start_timestamp, double end_timestamp);
    void startNewVideoSegment(double start_timestamp);
    void finalizeCurrentVideoSegment(double end_timestamp);
    
    ros::NodeHandle nh_;
    ros::Subscriber timestamp_sub_;
    ros::Subscriber image_sub_;
    ros::Publisher image_pub_;
    
    std::thread recording_thread_;
    std::thread playback_thread_;
    
    std::queue<sensor_msgs::CompressedImage> image_queue_;
    std::mutex image_mutex_;
    std::condition_variable image_cv_;
    
    std::queue<std::pair<double, double>> timestamp_queue_;
    std::mutex timestamp_mutex_;
    
    sqlite3* db_;
    std::string base_dir_;
    std::string db_path_;
    
    std::atomic<bool> stop_threads_{false};
    std::atomic<bool> is_recording_{false};
    std::atomic<size_t> messages_recorded_{0};
    std::atomic<size_t> messages_played_{0};
    
    std::map<std::string, std::string> config_;
    
    // 视频录制相关变量
    cv::VideoWriter video_writer_;
    std::string current_video_path_;
    double current_segment_start_time_;
    double current_segment_end_time_;
    int current_segment_frame_count_;
    cv::Size frame_size_;
    double fps_;
    int segment_duration_; // 视频分段时长（秒）
};

#endif // CONCURRENT_RECORDER_H