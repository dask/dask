#!bin/bash

set -e

export HADOOP_PREFIX=/usr/local/hadoop
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

service sshd start

rm -f /tmp/*.pid
$HADOOP_PREFIX/sbin/start-dfs.sh

echo "--"
echo "-- HDFS started!"
echo "--"

# Wait for nodes to be fully initialized
sleep 5
touch /host/hdfs-initialized

# Stay alive
sleep infinity
