#!/usr/bin/env python
import rospy
import rosbag
import os
import time
from datetime import datetime
import argparse
import threading
from std_msgs.msg import String
from sensor_msgs.msg import Image

class DataPlayer:
    def __init__(self):
        rospy.init_node('data_player')
        
        self.bag_files = []
        self.current_bag = None
        self.play_rate = rospy.get_param('~play_rate', 1.0)
        self.loop = rospy.get_param('~loop', False)
        
        # 发布器
        self.publishers = {}
        
    def load_bag_files(self, directory, start_time=None, end_time=None):
        """加载指定时间范围内的bag文件"""
        if not os.path.exists(directory):
            rospy.logerr(f"Directory {directory} does not exist")
            return False
        
        # 获取所有bag文件并按时间排序
        bag_files = [f for f in os.listdir(directory) if f.endswith('.bag')]
        bag_files.sort()
        
        for bag_file in bag_files:
            file_path = os.path.join(directory, bag_file)
            try:
                # 检查文件时间戳
                file_time = self.get_file_time(bag_file)
                if start_time and file_time < start_time:
                    continue
                if end_time and file_time > end_time:
                    continue
                
                self.bag_files.append(file_path)
            except:
                continue
        
        return len(self.bag_files) > 0
    
    def get_file_time(self, filename):
        """从文件名中提取时间戳"""
        parts = filename.split('_')
        if len(parts) >= 3:
            time_str = parts[1] + '_' + parts[2].split('.')[0]
            return datetime.strptime(time_str, "%Y%m%d_%H%M%S")
        return datetime.min
    
    def setup_publishers(self, topics):
        """为bag文件中的话题创建发布器"""
        for topic, msg_type in topics:
            if msg_type == 'sensor_msgs/Image':
                self.publishers[topic] = rospy.Publisher(topic, Image, queue_size=10)
            else:
                self.publishers[topic] = rospy.Publisher(topic, String, queue_size=10)
    
    def play_bag(self, bag_file):
        """播放单个bag文件"""
        try:
            bag = rosbag.Bag(bag_file)
            topics = bag.get_type_and_topic_info().topics
            
            # 设置发布器
            self.setup_publishers([(topic, info.msg_type) for topic, info in topics.items()])
            
            start_time = bag.get_start_time()
            for topic, msg, t in bag.read_messages():
                if rospy.is_shutdown():
                    break
                
                # 计算实际播放时间
                elapsed = (t.to_sec() - start_time) / self.play_rate
                time.sleep(elapsed)
                
                if topic in self.publishers:
                    self.publishers[topic].publish(msg)
            
            bag.close()
            
        except Exception as e:
            rospy.logerr(f"Error playing bag {bag_file}: {e}")
    
    def play_all(self):
        """播放所有加载的bag文件"""
        for bag_file in self.bag_files:
            if rospy.is_shutdown():
                break
            rospy.loginfo(f"Playing {bag_file}")
            self.play_bag(bag_file)
        
        if self.loop:
            self.play_all()
    
    def run(self, data_dir, start_time=None, end_time=None):
        if self.load_bag_files(data_dir, start_time, end_time):
            play_thread = threading.Thread(target=self.play_all)
            play_thread.start()
            rospy.spin()
        else:
            rospy.logerr("No bag files found in the specified time range")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ROS Data Player')
    parser.add_argument('--data_dir', type=str, required=True, help='Data directory')
    parser.add_argument('--start_time', type=str, help='Start time (YYYYMMDD_HHMMSS)')
    parser.add_argument('--end_time', type=str, help='End time (YYYYMMDD_HHMMSS)')
    parser.add_argument('--rate', type=float, default=1.0, help='Playback rate')
    parser.add_argument('--loop', action='store_true', help='Loop playback')
    
    args = parser.parse_args()
    
    player = DataPlayer()
    player.play_rate = args.rate
    player.loop = args.loop
    
    # 转换时间字符串
    start_dt = datetime.strptime(args.start_time, "%Y%m%d_%H%M%S") if args.start_time else None
    end_dt = datetime.strptime(args.end_time, "%Y%m%d_%H%M%S") if args.end_time else None
    
    player.run(args.data_dir, start_dt, end_dt)