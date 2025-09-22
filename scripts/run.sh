#!/bin/bash

# 获取脚本的目录
SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../"; pwd)"

echo $SCRIPT_DIR $ROOT_DIR
cd $ROOT_DIR/../../
# 转到catkin工作空间

if catkin_make -j8;then
    source devel/setup.bash
    roslaunch ros_data_recorder run.launch test_recorder:=false
fi


