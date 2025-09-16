#include <ros/ros.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <std_msgs/Float32MultiArray.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <sensor_msgs/image_encodings.h>
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
#include <opencv2/imgcodecs.hpp>
#include <sqlite3.h>
#include <yaml-cpp/yaml.h>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <filesystem>
#include <chrono>
#include <ctime>

namespace fs = std::filesystem;

class DataRecorder
{
public:
    DataRecorder() : nh_("~")
    {
        // 加载配置
        std::string config_file;
        nh_.param("config_file", config_file, std::string("config/config.yaml"));
        loadConfig(config_file);
        
        // 获取数据目录
        nh_.param("data_dir", base_dir_, std::string("/data/ros_recordings"));
        db_path_ = base_dir_ + "/recordings.db";
        
        // 创建数据目录
        if (!fs::exists(base_dir_))
        {
            fs::create_directories(base_dir_);
        }
        
        initDatabase();
        
        // 设置发布器
        image_pub_ = nh_.advertise<sensor_msgs::Image>("/recordImage", 10);
        
        // 订阅时间戳控制消息
        timestamp_sub_ = nh_.subscribe("/recordTimes", 10, &DataRecorder::timestampCallback, this);
        
        // 订阅压缩图像话题
        image_sub_ = nh_.subscribe("/detectImage/compressed", 10, &DataRecorder::imageCallback, this);
        
        // 启动工作线程
        recording_thread_ = std::thread(&DataRecorder::recordingWorker, this);
        playback_thread_ = std::thread(&DataRecorder::playbackWorker, this);
        
        ROS_INFO("Data recorder initialized");
    }
    
    ~DataRecorder()
    {
        stop_threads_ = true;
        if (recording_thread_.joinable()) recording_thread_.join();
        if (playback_thread_.joinable()) playback_thread_.join();
        
        if (current_bag_ && current_bag_->isOpen())
        {
            try
            {
                uint64_t file_size = fs::file_size(current_bag_->getFileName());
                double bag_start = bag_start_time_.toSec();
                double bag_end = ros::Time::now().toSec();
                addRecordingToDB(fs::path(current_bag_->getFileName()).filename().string(), 
                               bag_start, bag_end, file_size);
                current_bag_->close();
            }
            catch (const std::exception& e)
            {
                ROS_ERROR("Error during shutdown: %s", e.what());
            }
        }
    }
    
    void run()
    {
        ros::spin();
    }

private:
    void timestampCallback(const std_msgs::Float32MultiArray::ConstPtr& msg)
    {
        if (msg->data.size() >= 2)
        {
            double start_timestamp = msg->data[0] - 10.0;
            double end_timestamp = msg->data[1] + 10.0;
            
            if (start_timestamp > 0 && end_timestamp > start_timestamp)
            {
                std::lock_guard<std::mutex> lock(timestamp_mutex_);
                timestamp_queue_.push({start_timestamp, end_timestamp});
                ROS_INFO("Received playback request: %.6f to %.6f", start_timestamp, end_timestamp);
            }
            else
            {
                ROS_WARN("Invalid timestamp range");
            }
        }
        else
        {
            ROS_WARN("Invalid timestamp message format");
        }
    }
    
    void imageCallback(const sensor_msgs::CompressedImage::ConstPtr& msg)
    {
        if (is_recording_)
        {
            std::lock_guard<std::mutex> lock(image_mutex_);
            image_queue_.push(*msg);
        }
    }
    
    void recordingWorker()
    {
        ROS_INFO("Recording worker started");
        
        while (!stop_threads_ && ros::ok())
        {
            sensor_msgs::CompressedImage msg;
            {
                std::unique_lock<std::mutex> lock(image_mutex_);
                if (image_queue_.empty())
                {
                    lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
                msg = image_queue_.front();
                image_queue_.pop();
            }
            
            processRecordingMessage(msg);
        }
    }
    
    void playbackWorker()
    {
        ROS_INFO("Playback worker started");
        
        while (!stop_threads_ && ros::ok())
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
    
    void processRecordingMessage(const sensor_msgs::CompressedImage& msg)
    {
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
    
    void playbackFromBag(double start_timestamp, double end_timestamp)
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
                ros::Time bag_start = full_view.getBeginTime();
                ros::Time bag_end = full_view.getEndTime();
                double bag_start_sec = bag_start.toSec();
                double bag_end_sec = bag_end.toSec();
                
                // Then use bag_start_sec and bag_end_sec in your calculations:
                double play_start = std::max(start_timestamp, bag_start_sec);
                double play_end = std::min(end_timestamp, bag_end_sec);
                
                if (play_start >= play_end) continue;
                
                ROS_INFO("Playing %s from %.6f to %.6f", bag_file.c_str(), play_start, play_end);
                
                rosbag::View view(bag, rosbag::TopicQuery("/detectImage/compressed"), 
                               ros::Time().fromSec(play_start), ros::Time().fromSec(play_end));
                
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
                            }
                        }
                        catch (const std::exception& e)
                        {
                            ROS_ERROR("Error processing image: %s", e.what());
                        }
                    }
                    
                    std::this_thread::sleep_for(std::chrono::milliseconds(33));
                }
                
                bag.close();
            }
            catch (const std::exception& e)
            {
                ROS_ERROR("Error playing bag %s: %s", bag_file.c_str(), e.what());
            }
        }
    }
    
    std::vector<std::string> findBagFiles(double start_ts, double end_ts)
    {
        std::vector<std::string> result;
        sqlite3* db;
        
        if (sqlite3_open(db_path_.c_str(), &db) != SQLITE_OK)
        {
            ROS_ERROR("Cannot open database: %s", sqlite3_errmsg(db));
            return result;
        }
        
        const char* sql = "SELECT filename FROM recordings "
                         "WHERE (start_time <= ? AND end_time >= ?) "
                         "OR (start_time <= ? AND end_time >= ?) "
                         "OR (start_time >= ? AND end_time <= ?) "
                         "ORDER BY start_time";
        
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK)
        {
            sqlite3_bind_double(stmt, 1, end_ts);
            sqlite3_bind_double(stmt, 2, end_ts);
            sqlite3_bind_double(stmt, 3, start_ts);
            sqlite3_bind_double(stmt, 4, start_ts);
            sqlite3_bind_double(stmt, 5, start_ts);
            sqlite3_bind_double(stmt, 6, end_ts);
            
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                const char* filename = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                std::string filepath = base_dir_ + "/" + filename;
                if (fs::exists(filepath))
                {
                    result.push_back(filepath);
                }
                else
                {
                    ROS_WARN("Bag file not found: %s", filepath.c_str());
                }
            }
        }
        
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return result;
    }
    
    void createNewBag()
    {
        if (current_bag_ && current_bag_->isOpen())
        {
            try
            {
                uint64_t file_size = fs::file_size(current_bag_->getFileName());
                double bag_start = bag_start_time_.toSec();
                double bag_end = ros::Time::now().toSec();
                addRecordingToDB(fs::path(current_bag_->getFileName()).filename().string(), 
                               bag_start, bag_end, file_size);
                current_bag_->close();
            }
            catch (const std::exception& e)
            {
                ROS_ERROR("Error closing bag: %s", e.what());
            }
        }
        
        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_time);
        
        int current_hour = now_tm->tm_hour;
        int next_hour = (current_hour + 1) % 24;
        
        char date_str[9];
        std::strftime(date_str, sizeof(date_str), "%Y%m%d", now_tm);
        
        std::string filename = "recording_" + std::string(date_str) + "_" + 
                             (current_hour < 10 ? "0" : "") + std::to_string(current_hour) + "_" +
                             (next_hour < 10 ? "0" : "") + std::to_string(next_hour) + ".bag";
        std::string filepath = base_dir_ + "/" + filename;
        
        try
        {
            current_bag_ = std::make_unique<rosbag::Bag>(filepath, rosbag::bagmode::Write);
            bag_start_time_ = ros::Time::now();
            
            // 计算下一个整点时间
            std::tm next_hour_tm = *now_tm;
            next_hour_tm.tm_hour = next_hour;
            next_hour_tm.tm_min = 0;
            next_hour_tm.tm_sec = 0;
            std::time_t next_hour_time = std::mktime(&next_hour_tm);
            
            addRecordingToDB(filename, bag_start_time_.toSec(), next_hour_time, 0);
            ROS_INFO("Created new bag file: %s", filepath.c_str());
        }
        catch (const std::exception& e)
        {
            ROS_ERROR("Error creating bag file: %s", e.what());
            current_bag_.reset();
        }
    }
    
    void initDatabase()
    {
        sqlite3* db;
        if (sqlite3_open(db_path_.c_str(), &db) == SQLITE_OK)
        {
            const char* sql = "CREATE TABLE IF NOT EXISTS recordings ("
                           "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                           "filename TEXT NOT NULL,"
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
    
    void addRecordingToDB(const std::string& filename, double start_time, double end_time, uint64_t file_size)
    {
        sqlite3* db;
        if (sqlite3_open(db_path_.c_str(), &db) == SQLITE_OK)
        {
            const char* sql = "INSERT INTO recordings (filename, start_time, end_time, duration, file_size) "
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
                    ROS_ERROR("Failed to insert recording: %s", sqlite3_errmsg(db));
                }
            }
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }
    
    void loadConfig(const std::string& config_file)
    {
        try
        {
            YAML::Node config = YAML::LoadFile(config_file);
            config_["compression_quality"] = config["compression_quality"].as<int>(90);
            config_["image_compression_format"] = config["image_compression_format"].as<std::string>("jpeg");
            ROS_INFO("Configuration loaded successfully");
        }
        catch (const std::exception& e)
        {
            ROS_WARN("Failed to load config: %s. Using default settings.", e.what());
            config_["compression_quality"] = 90;
            config_["image_compression_format"] = "jpeg";
        }
    }
    
    ros::NodeHandle nh_;
    ros::Subscriber timestamp_sub_;
    ros::Subscriber image_sub_;
    ros::Publisher image_pub_;
    
    std::unique_ptr<rosbag::Bag> current_bag_;
    ros::Time bag_start_time_;
    
    std::string base_dir_;
    std::string db_path_;
    std::map<std::string, std::string> config_;
    
    std::thread recording_thread_;
    std::thread playback_thread_;
    std::atomic<bool> stop_threads_{false};
    std::atomic<bool> is_recording_{true};
    
    std::queue<std::pair<double, double>> timestamp_queue_;
    std::queue<sensor_msgs::CompressedImage> image_queue_;
    std::mutex timestamp_mutex_;
    std::mutex image_mutex_;
};

int main(int argc, char** argv)
{
    ros::init(argc, argv, "data_recorder");
    DataRecorder recorder;
    recorder.run();
    return 0;
}