FROM sequenceiq/hadoop-docker:2.7.1

ENV HADOOP_PREFIX=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop

ADD docker-files/start.sh /tmp/start.sh
ADD docker-files/*.xml $HADOOP_CONF_DIR/

VOLUME /host

EXPOSE 8020
EXPOSE 50070

CMD ["bash", "/tmp/start.sh"]

# Build:
#
# $ docker build -t distributed-hdfs-test .

# Configure host to access container HDFS:
#
# $ sudo bash -c 'echo "127.0.0.1 hdfs-container" >> /etc/hosts'

# Execution:
#
# $ rm -f hdfs-initialized
# $ docker run -d -h hdfs-container -v$(pwd):/host -p8020:8020 -p 50070:50070 distributed-hdfs-test
