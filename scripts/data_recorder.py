#!/usr/bin/env python3
import rospy
import rosbag
import os
import time
from datetime import datetime
from std_msgs.msg import String, Int32MultiArray
from sensor_msgs.msg import Image, CompressedImage
import cv2
from cv_bridge import CvBridge
import threading
import yaml
import json
import sqlite3
import queue
import glob
import multiprocessing as mp

class DataRecorder:
    def __init__(self):
        rospy.init_node('data_recorder', anonymous=True)
        
        # 加载配置
        self.config_file = rospy.get_param('~config_file', 'config/config.yaml')
        self.load_config()
        
        self.bridge = CvBridge()
        self.current_bag = None
        self.bag_start_time = None
        self.bag_duration = 3600  # 1小时分割一次
        self.bag_counter = 0
        self.base_dir = rospy.get_param('~data_dir', '/data/ros_recordings')
        self.db_path = os.path.join(self.base_dir, 'recordings.db')
        
        # 创建数据目录和数据库
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        self.init_database()
        
        # 线程间通信队列
        self.timestamp_queue = mp.Queue()
        self.playback_queue = mp.Queue()
        
        # 录制状态
        self.is_recording = True  # 默认一直录制
        
        # 设置发布器 - 用于回放
        self.image_publisher = rospy.Publisher('/recordImage', Image, queue_size=10)
        
        # 使用多进程队列替代线程队列
        self.timestamp_queue = mp.Queue()
        self.playback_queue = mp.Queue()
        
        # 启动录制进程
        self.recording_process = mp.Process(target=self.recording_worker)
        self.recording_process.daemon = True
        self.recording_process.start()
        
        # 启动回放进程
        self.playback_process = mp.Process(target=self.playback_worker)
        self.playback_process.daemon = True
        self.playback_process.start()
        
        # 订阅时间戳控制消息
        rospy.Subscriber('/recordTimes', Int32MultiArray, self.timestamp_callback)
        
        # 订阅/detectImage/compressed话题 - 录制线程处理
        rospy.Subscriber('/detectImage/compressed', CompressedImage, self.detect_image_callback)
        
        rospy.loginfo("Multi-thread data recorder initialized")
    
    def init_database(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS recordings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                start_time REAL NOT NULL,
                end_time REAL NOT NULL,
                duration REAL,
                file_size INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
    
    def add_recording_to_db(self, filename, start_time, end_time, file_size):
        """添加录制记录到数据库"""
        duration = end_time - start_time
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO recordings (filename, start_time, end_time, duration, file_size)
            VALUES (?, ?, ?, ?, ?)
        ''', (filename, start_time, end_time, duration, file_size))
        conn.commit()
        conn.close()
    
    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
            rospy.loginfo("Configuration loaded successfully")
        except Exception as e:
            rospy.logwarn(f"Failed to load config: {e}. Using default settings.")
            self.config = {
                'compression_quality': 90,
                'image_compression_format': 'jpeg'
            }
    
    def timestamp_callback(self, msg):
        """处理时间戳控制消息"""
        if len(msg.data) >= 2:
            # 时间两边延长10秒
            start_timestamp = msg.data[0] - 10
            end_timestamp = msg.data[1] + 10
            
            if start_timestamp > 0 and end_timestamp > start_timestamp:
                self.timestamp_queue.put((start_timestamp, end_timestamp))
                rospy.loginfo(f"Received playback request: {start_timestamp} to {end_timestamp}")
            else:
                rospy.logwarn("Invalid timestamp range")
        else:
            rospy.logwarn("Invalid timestamp message format")
    
    def detect_image_callback(self, msg):
        """处理/detectImage/compressed话题 - 只是将消息放入队列"""
        if self.is_recording:
            try:
                # 使用深拷贝避免线程问题
                import copy
                msg_copy = copy.deepcopy(msg)
                self.playback_queue.put(('record', msg_copy))
            except Exception as e:
                rospy.logerr(f"Error queuing compressed image message: {e}")
    
    def recording_worker(self):
        """录制线程工作函数"""
        rospy.loginfo("Recording worker started")
        
        while not rospy.is_shutdown():
            try:
                # 从队列获取消息
                item = self.playback_queue.get(timeout=1.0)
                if item[0] == 'record':
                    self.process_recording_message(item[1])
                
            except queue.Empty:
                continue
            except Exception as e:
                rospy.logerr(f"Error in recording worker: {e}")
    
    def process_recording_message(self, msg):
        """处理录制消息"""
        if not self.current_bag:
            self.create_new_bag()
        
        try:
            # 直接保存压缩图像到bag文件
            self.current_bag.write('/detectImage/compressed', msg, 
                                msg.header.stamp if msg.header.stamp else rospy.Time.now())
            
        except Exception as e:
            rospy.logerr(f"Error writing compressed image to bag: {e}")
    
    def playback_worker(self):
        """回放线程工作函数"""
        rospy.loginfo("Playback worker started")
        
        while not rospy.is_shutdown():
            try:
                # 从队列获取时间戳请求
                start_ts, end_ts = self.timestamp_queue.get(timeout=1.0)
                self.playback_from_bag(start_ts, end_ts)
                
            except queue.Empty:
                continue
            except Exception as e:
                rospy.logerr(f"Error in playback worker: {e}")
    
    def playback_from_bag(self, start_timestamp, end_timestamp):
        """从bag文件中回放指定时间段的图像"""
        rospy.loginfo(f"Playing back from {start_timestamp} to {end_timestamp}")
        
        # 查找匹配的bag文件
        bag_files = self.find_bag_files(start_timestamp, end_timestamp)
        
        if not bag_files:
            rospy.logwarn("No bag files found for the specified time range")
            return
        
        for bag_file in bag_files:
            if rospy.is_shutdown():
                break
            
            try:
                bag = rosbag.Bag(bag_file)
                
                # 获取bag文件的时间范围
                bag_start = bag.get_start_time()
                bag_end = bag.get_end_time()
                
                # 计算实际播放的时间范围
                play_start = max(start_timestamp, bag_start)
                play_end = min(end_timestamp, bag_end)
                
                if play_start >= play_end:
                    bag.close()
                    continue
                
                rospy.loginfo(f"Playing {bag_file} from {play_start} to {play_end}")
                
                # 播放图像消息
                for topic, msg, t in bag.read_messages(
                    topics=['/detectImage/compressed'],
                    start_time=rospy.Time.from_sec(play_start),
                    end_time=rospy.Time.from_sec(play_end)
                ):
                    if rospy.is_shutdown():
                        break
                    
                    # 处理压缩图像话题
                    if topic == '/detectImage/compressed':
                        # 使用cv_bridge将CompressedImage转换为Image消息
                        try:
                            # 将压缩图像数据转换为numpy数组
                            import numpy as np
                            np_arr = np.frombuffer(msg.data, np.uint8)
                            
                            # 解码压缩图像
                            cv_image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                            
                            if cv_image is not None:
                                # 将OpenCV图像转换为ROS Image消息
                                image_msg = self.bridge.cv2_to_imgmsg(cv_image, encoding="bgr8")
                                image_msg.header = msg.header
                                self.image_publisher.publish(image_msg)
                            else:
                                rospy.logwarn("Failed to decode compressed image")
                                
                        except Exception as e:
                            rospy.logerr(f"Error converting compressed image: {e}")
                    
                    # 控制播放速率
                    time.sleep(0.033)  # ~30fps
                
                bag.close()
                
            except Exception as e:
                rospy.logerr(f"Error playing bag {bag_file}: {e}")
    
    def find_bag_files(self, start_timestamp, end_timestamp):
        """从sqlite数据库中查找包含指定时间段的bag文件"""
        bag_files = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 查询包含指定时间段的bag文件
            cursor.execute('''
                SELECT filename FROM recordings 
                WHERE (start_time <= ? AND end_time >= ?) 
                   OR (start_time <= ? AND end_time >= ?)
                   OR (start_time >= ? AND end_time <= ?)
                ORDER BY start_time
            ''', (end_timestamp, end_timestamp, 
                  start_timestamp, start_timestamp,
                  start_timestamp, end_timestamp))
            
            results = cursor.fetchall()
            for row in results:
                filename = row[0]
                filepath = os.path.join(self.base_dir, filename)
                if os.path.exists(filepath):
                    bag_files.append(filepath)
                else:
                    rospy.logwarn(f"Bag file not found: {filepath}")
            
            conn.close()
            
        except Exception as e:
            rospy.logerr(f"Error querying database for bag files: {e}")
            return []
        
        return bag_files
    
    def check_and_open_existing_bag(self):
        """启动时检查是否有可追加的bag文件"""
        current_time = rospy.Time.now().to_sec()
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 查询当前时间在哪个bag文件的时间范围内
            cursor.execute('''
                SELECT filename, start_time, end_time FROM recordings 
                WHERE start_time <= ? AND end_time >= ?
                ORDER BY start_time DESC
            ''', (current_time, current_time))
            
            result = cursor.fetchone()
            if result:
                filename, start_time, end_time = result
                filepath = os.path.join(self.base_dir, filename)
                
                if os.path.exists(filepath):
                    # 以追加模式打开现有bag文件
                    self.current_bag = rosbag.Bag(filepath, 'a')
                    self.bag_start_time = rospy.Time.from_sec(start_time)
                    
                    # 更新数据库中的end_time为当前时间
                    cursor.execute('''
                        UPDATE recordings SET end_time = ? 
                        WHERE filename = ?
                    ''', (current_time, filename))
                    conn.commit()
                    
                    rospy.loginfo(f"Appending to existing bag file: {filename}")
                    rospy.loginfo(f"Original time range: {start_time} to {end_time}")
                    rospy.loginfo(f"Now appending from: {current_time}")
                
            conn.close()
            
        except Exception as e:
            rospy.logerr(f"Error checking existing bags: {e}")
    
    def create_new_bag(self):
        if self.current_bag:
            try:
                file_size = os.path.getsize(self.current_bag.filename)
                bag_start = self.bag_start_time.to_sec()
                bag_end = rospy.Time.now().to_sec()
                self.add_recording_to_db(
                    os.path.basename(self.current_bag.filename),
                    bag_start,
                    bag_end,
                    file_size
                )
                self.current_bag.close()
            except Exception as e:
                rospy.logerr(f"Error closing bag: {e}")
        
        # 获取当前时间的整点小时和下一个整点小时
        now = datetime.now()
        current_hour = now.hour
        next_hour = (current_hour + 1) % 24
        
        # 格式化文件名：recording_年月日_当前小时_下一个小时.bag
        date_str = now.strftime("%Y%m%d")
        filename = f"recording_{date_str}_{current_hour:02d}_{next_hour:02d}.bag"
        filepath = os.path.join(self.base_dir, filename)
        
        # 计算下一个整点的时间
        next_hour_time = now.replace(minute=0, second=0, microsecond=0)
        if next_hour > current_hour:
            next_hour_time = next_hour_time.replace(hour=next_hour)
        else:
            # 如果是23点到0点的情况
            next_hour_time = next_hour_time.replace(day=next_hour_time.day + 1, hour=0)
        
        # 设置bag的结束时间
        self.bag_end_time = next_hour_time
        
        try:
            self.current_bag = rosbag.Bag(filepath, 'w')
            self.bag_start_time = rospy.Time.now()
            
            # 立即添加到数据库（初始记录，end_time设为start_time）
            self.add_recording_to_db(
                filename,
                self.bag_start_time.to_sec(),
                self.bag_start_time.to_sec(),  # 初始end_time等于start_time
                0  # 初始文件大小为0
            )
            
            rospy.loginfo(f"Created new bag file: {filepath}")
            rospy.loginfo(f"This bag will contain data until: {next_hour_time}")
        except Exception as e:
            rospy.logerr(f"Error creating bag file: {e}")
            self.current_bag = None
    
    def check_bag_duration(self):
        """检查是否需要创建新的bag文件（到达整点小时）"""
        if self.current_bag and datetime.now() >= self.bag_end_time:
            self.create_new_bag()
        
        # 重新设置定时器（每分钟检查一次）
        self.timer = threading.Timer(60.0, self.check_bag_duration)
        self.timer.start()
    
    def should_record_in_current_bag(self, msg_time):
        """检查消息是否应该记录在当前bag文件中"""
        if not self.current_bag:
            return True
            
        current_time = msg_time if msg_time else rospy.Time.now()
        current_time_dt = datetime.fromtimestamp(current_time.to_sec())
        
        # 检查消息时间是否在当前bag的时间范围内
        return current_time_dt < self.bag_end_time
    
    def image_callback(self, msg, topic):
        if not self.should_record(msg.header.stamp if msg.header.stamp else None):
            return
            
        # 检查是否需要创建新的bag文件
        if not self.current_bag or not self.should_record_in_current_bag(msg.header.stamp if msg.header.stamp else None):
            self.create_new_bag()
        
        try:
            # 压缩图像
            compressed_msg = self.compress_image(msg)
            if compressed_msg:
                compressed_topic = topic + '/compressed'
                self.current_bag.write(compressed_topic, compressed_msg, 
                                    msg.header.stamp if msg.header.stamp else rospy.Time.now())
            
            # 保存原始图像
            # self.current_bag.write(topic, msg, msg.header.stamp if msg.header.stamp else rospy.Time.now())
            
        except Exception as e:
            rospy.logerr(f"Error writing image to bag: {e}")

    def compress_image(self, image_msg):
        """压缩图像消息"""
        try:
            cv_image = self.bridge.imgmsg_to_cv2(image_msg, desired_encoding='bgr8')
            
            compression_format = self.config.get('image_compression_format', 'jpeg')
            quality = self.config.get('compression_quality', 90)
            
            if compression_format.lower() == 'jpeg':
                encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
                result, compressed_data = cv2.imencode('.jpg', cv_image, encode_param)
            elif compression_format.lower() == 'png':
                encode_param = [int(cv2.IMWRITE_PNG_COMPRESSION), 9 - quality // 10]
                result, compressed_data = cv2.imencode('.png', cv_image, encode_param)
            else:
                return None
            
            if result:
                compressed_msg = CompressedImage()
                compressed_msg.header = image_msg.header
                compressed_msg.format = compression_format
                compressed_msg.data = compressed_data.tobytes()
                return compressed_msg
            
        except Exception as e:
            rospy.logerr(f"Error compressing image: {e}")
        
        return None
    
    def run(self):
        rospy.spin()
        
        # 清理资源
        if self.current_bag:
            try:
                file_size = os.path.getsize(self.current_bag.filename)
                bag_start = self.bag_start_time.to_sec()
                bag_end = rospy.Time.now().to_sec()
                self.add_recording_to_db(
                    os.path.basename(self.current_bag.filename),
                    bag_start,
                    bag_end,
                    file_size
                )
                self.current_bag.close()
            except Exception as e:
                rospy.logerr(f"Error during shutdown: {e}")

if __name__ == '__main__':
    try:
        recorder = DataRecorder()
        recorder.run()
    except rospy.ROSInterruptException:
        pass