#!/bin/bash

# Start HDFS
sudo service hadoop-hdfs-namenode start
sudo service hadoop-hdfs-datanode start

echo "Ready"

# Block
sleep infinity
