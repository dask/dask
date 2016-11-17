#!/bin/bash

HOSTDIR=$(pwd)
INIT_MARKER=$HOSTDIR/hdfs-initialized

# Remove initialization marker
rm -f $INIT_MARKER

CONTAINER_ID=$(docker run -d -h hdfs-container -v$HOSTDIR:/host -p8020:8020 -p 50070:50070 distributed-hdfs-test)

if [ $? -ne 0 ]; then
    echo "Failed starting HDFS container"
    exit 1
fi
echo "Started HDFS container: $CONTAINER_ID"

# CONTAINER_ID=$1
CHECK_RUNNING="docker top $CONTAINER_ID"

# Wait for initialization
while [[ $($CHECK_RUNNING) ]] && [[ ! -f $INIT_MARKER ]]
do
    sleep 1
done

# Error out if the container failed starting
if [[ ! $($CHECK_RUNNING) ]]; then
    echo "HDFS startup failed! Logs follow"
    echo "-------------------------------------------------"
    docker logs $CONTAINER_ID
    echo "-------------------------------------------------"
    exit 1
fi
