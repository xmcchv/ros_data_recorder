#!/usr/bin/env python3
import rospy
import time
from std_msgs.msg import Int32MultiArray
from sensor_msgs.msg import Image

class TestRecorder:
    def __init__(self):
        rospy.init_node('test_recorder', anonymous=True)
        
        # 发布器 - 发布时间戳控制消息
        self.timestamp_publisher = rospy.Publisher('/recordTimes', Int32MultiArray, queue_size=10)
        
        # 订阅器 - 订阅回放的图像消息
        rospy.Subscriber('/recordImage', Image, self.image_callback)
        
        # 测试计数器
        self.received_images = 0
        
        rospy.loginfo("Test recorder initialized")
    
    def image_callback(self, msg):
        """处理接收到的图像消息"""
        self.received_images += 1
        rospy.loginfo(f"Received image #{self.received_images}: "
                     f"timestamp={msg.header.stamp.secs}, "
                     f"resolution={msg.width}x{msg.height}, "
                     f"encoding={msg.encoding}")
    
    def send_test_timestamps(self):
        """发送测试时间戳"""
        # 获取当前时间戳
        current_time = int(time.time())
        
        # 测试时间范围：当前时间前60秒到前30秒
        start_timestamp = current_time - 60
        end_timestamp = current_time - 30
        
        # 创建时间戳消息
        timestamp_msg = Int32MultiArray()
        timestamp_msg.data = [start_timestamp, end_timestamp]
        
        # 发布时间戳
        self.timestamp_publisher.publish(timestamp_msg)
        rospy.loginfo(f"Sent timestamps: {start_timestamp} to {end_timestamp}")
        
        return start_timestamp, end_timestamp
    
    def run_test(self):
        """运行测试"""
        rospy.loginfo("Starting test...")
        
        # 等待1秒确保订阅者就绪
        rospy.sleep(1.0)
        
        # 发送测试时间戳
        start_ts, end_ts = self.send_test_timestamps()
        
        # 等待回放完成（假设最多等待10秒）
        timeout = 10.0
        start_time = time.time()
        
        while not rospy.is_shutdown() and (time.time() - start_time) < timeout:
            if self.received_images > 0:
                rospy.loginfo(f"Test successful! Received {self.received_images} images")
                return True
            rospy.sleep(0.1)
        
        if self.received_images == 0:
            rospy.logwarn("Test failed: No images received within timeout")
            return False
    
    def run(self):
        """运行测试节点"""
        # 运行一次测试
        success = self.run_test()
        
        if success:
            rospy.loginfo("=== TEST PASSED ===")
        else:
            rospy.logwarn("=== TEST FAILED ===")
        
        # 保持节点运行以便继续接收消息
        rospy.spin()

if __name__ == '__main__':
    try:
        tester = TestRecorder()
        tester.run()
    except rospy.ROSInterruptException:
        pass