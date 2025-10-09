#include "data_recorder/concurrent_recorder.h"
#include <ros/ros.h>
#include <csignal>
#include <atomic>

std::atomic<bool> g_shutdown_requested{false};

void printUsage(const std::string& programName)
{
    std::cout << "Usage: " << programName << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --help, -h        Show this help message" << std::endl;
}

void signalHandler(int signal)
{
    g_shutdown_requested = true;
    ros::shutdown();
}

int main(int argc, char** argv)
{
    // 注册信号处理函数
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    ros::init(argc, argv, "concurrent_data_recorder", ros::init_options::NoSigintHandler);
    
    printUsage(argv[0]);

    try
    {
        ConcurrentRecorder recorder;
        recorder.run();
    }
    catch (const std::exception& e)
    {
        ROS_ERROR("Fatal error: %s", e.what());
        return 1;
    }
    
    return 0;
}