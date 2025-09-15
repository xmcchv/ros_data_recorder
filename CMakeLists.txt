cmake_minimum_required(VERSION 2.8.3)
project(ros_data_recorder)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rospy
  std_msgs
  sensor_msgs
  image_transport
  cv_bridge
  message_filters
)

catkin_package()

install(PROGRAMS
  scripts/data_recorder.py
  scripts/data_player.py
  scripts/web_interface.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY templates
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY web_static
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)