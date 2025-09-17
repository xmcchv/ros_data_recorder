cmake_minimum_required(VERSION 3.1)
project(ros_data_recorder)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rospy
  std_msgs
  sensor_msgs
  image_transport
  cv_bridge
  message_filters
  rosbag
)

find_package(OpenCV REQUIRED)
find_package(SQLite3 REQUIRED)
set(YAML_CPP_INCLUDE_DIR "/usr/local/include/yaml-cpp")
set(YAML_CPP_LIBRARIES "/usr/local/lib/libyaml-cpp.a")

catkin_package(
  INCLUDE_DIRS include
  LIBRARIES ${PROJECT_NAME}
  CATKIN_DEPENDS roscpp std_msgs sensor_msgs
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
  ${OpenCV_INCLUDE_DIRS}
  ${SQLite3_INCLUDE_DIRS}
  ${YAML_CPP_INCLUDE_DIR}
)

# 设置C++17标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 创建C++可执行文件
add_library(${PROJECT_NAME}
  src/concurrent_recorder.cpp
)

target_link_libraries(${PROJECT_NAME}
  ${catkin_LIBRARIES}
  ${OpenCV_LIBRARIES}
  ${SQLite3_LIBRARIES}
  ${YAML_CPP_LIBRARIES}
)

add_executable(${PROJECT_NAME}_node
  src/main.cpp
)

target_link_libraries(${PROJECT_NAME}_node
  ${PROJECT_NAME}
  ${catkin_LIBRARIES}
)

install(TARGETS ${PROJECT_NAME} ${PROJECT_NAME}_node
  ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
)


add_executable(test_recorder src/test_recorder.cpp)
target_link_libraries(test_recorder
  ${catkin_LIBRARIES}
  ${PROJECT_NAME}
)
install(TARGETS test_recorder
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)