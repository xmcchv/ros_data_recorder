#include <ros/ros.h>
#include <std_msgs/Float32MultiArray.h>
#include <sensor_msgs/Image.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <opencv2/opencv.hpp>
#include <cv_bridge/cv_bridge.h>

class TestRecorder
{
public:
    TestRecorder() : nh_("~")
    {
        // 发布器
        timestamp_pub_ = nh_.advertise<std_msgs::Float32MultiArray>("/recordTimes", 10);
        
        // 订阅器
        image_sub_ = nh_.subscribe("/recordImage", 10, &TestRecorder::imageCallback, this);
        
        received_images_ = 0;
        ROS_INFO("Test recorder initialized");
    }
    
    void imageCallback(const sensor_msgs::Image::ConstPtr& msg)
    {
        int current_count = ++received_images_;
        ROS_INFO("Received image #%d: timestamp=%d, resolution=%dx%d, encoding=%s",
                current_count, msg->header.stamp.sec, 
                msg->width, msg->height, msg->encoding.c_str());
        // opencv可视化
        // cv::Mat image(cv_bridge::toCvShare(msg, sensor_msgs::image_encodings::BGR8)->image);
        // cv::imshow("Received Image", image);
        // cv::waitKey(1);
    }
    
    std::pair<double, double> sendTestTimestamps()
    {
        // auto current_time = std::chrono::system_clock::now().time_since_epoch().count() / 1e9;
        
        double start_timestamp = 1758015360.46; //current_time - 60.0;
        double end_timestamp = 1758015370.56; //current_time - 30.0;
        
        std_msgs::Float32MultiArray timestamp_msg;
        timestamp_msg.data = {static_cast<float>(start_timestamp), static_cast<float>(end_timestamp)};
        
        timestamp_pub_.publish(timestamp_msg);
        ROS_INFO("Sent timestamps with decimals: %.6f to %.6f", start_timestamp, end_timestamp);
        
        return {start_timestamp, end_timestamp};
    }
    
    bool runTest()
    {
        ROS_INFO("Starting test...");
        
        // 等待订阅者就绪
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        auto [start_ts, end_ts] = sendTestTimestamps();
        
        auto start_time = std::chrono::steady_clock::now();
        constexpr double timeout = 10.0;
        
        while (ros::ok() && 
               std::chrono::duration<double>(std::chrono::steady_clock::now() - start_time).count() < timeout)
        {
            int current_count = received_images_.load();
            if (current_count > 0)
            {
                ROS_INFO("Test successful! Received %d images", current_count);
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        int final_count = received_images_.load();
        if (final_count == 0)
        {
            ROS_WARN("Test failed: No images received within timeout");
        }
        return false;
    }
    
    void run()
    {
        bool success = runTest();
        
        if (success)
        {
            ROS_INFO("=== TEST PASSED ===");
        }
        else
        {
            ROS_WARN("=== TEST FAILED ===");
        }
        
        ros::spin();
    }

private:
    ros::NodeHandle nh_;
    ros::Publisher timestamp_pub_;
    ros::Subscriber image_sub_;
    std::atomic<int> received_images_{0};
};

int main(int argc, char** argv)
{
    ros::init(argc, argv, "test_recorder");
    TestRecorder tester;
    tester.run();
    return 0;
}