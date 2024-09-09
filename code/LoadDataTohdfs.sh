#!/bin/bash

input_file="/home/hiennv/code/data/yellow_data.parquet"

hdfs_destination="/user/root/rawdata"

if hadoop fs -test -e "$hdfs_destination"; then
    echo "hdfs path found"
else
    hadoop fs -mkdir -p "$hdfs_destination"
    if [ $? -eq 0 ]; then
        echo "oke"
    else
        echo "fail"
        exit 1
    fi
fi

hadoop fs -copyFromLocal "$input_file" "$hdfs_destination"

if [ $? -eq 0 ]; then
    echo "push oke"
else
    echo "fail"
fi

