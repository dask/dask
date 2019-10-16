#!/bin/bash

# Start HDFS
sudo service hadoop-hdfs-namenode start
sudo service hadoop-hdfs-datanode start

echo "HDFS Started"

if [[ $1 == "-d" ]]; then
    # Running as a daemon, indicate to host that hdfs is started
    touch /working/hdfs-initialized-indicator
    sleep infinity
fi

if [[ $1 == "-bash" ]]; then
    /bin/bash
fi
