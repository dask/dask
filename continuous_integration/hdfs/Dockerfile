FROM ubuntu:xenial

RUN apt-get update && \
    apt-get install -y -q curl bzip2 git && \
    rm -rf /var/lib/apt/lists/*

# Install CDH5 in a single node: Pseudo Distributed
# Docs: https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cdh_qs_yarn_pseudo.html 
ADD docker-files/cloudera.pref /etc/apt/preferences.d/cloudera.pref
RUN curl -s https://archive.cloudera.com/cdh5/ubuntu/xenial/amd64/cdh/archive.key | apt-key add - && \
    echo 'deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/xenial/amd64/cdh xenial-cdh5 contrib' > /etc/apt/sources.list.d/cloudera.list && \
    echo 'deb-src http://archive.cloudera.com/cdh5/ubuntu/xenial/amd64/cdh xenial-cdh5 contrib' >> /etc/apt/sources.list.d/cloudera.list && \
    apt-get update && \
    apt-get install -y -q sudo openjdk-8-jre-headless hadoop-conf-pseudo libhdfs0 && \
    sudo -u hdfs hdfs namenode -format -force && \
    for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done && \
    bash /usr/lib/hadoop/libexec/init-hdfs.sh && \
    rm -rf /var/lib/apt/lists/*

# Needed for hdfs3
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml

# Install conda & build conda environments:
# py35 is root environment
# py27 gets own environment
RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh && \
    /bin/bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh && \
    /opt/conda/bin/conda install -y -q libhdfs3 pytest flake8 black -c conda-forge && \
    /opt/conda/bin/conda create -y -n py27 python=2.7 && \
    /opt/conda/bin/conda install -y -q -n py27 libhdfs3 pytest -c conda-forge

ENV PATH /opt/conda/bin:$PATH

# Notifies the dask tests to run
ENV DASK_RUN_HDFS_TESTS true

# Needed for libhdfs
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre/

EXPOSE 8020 50070

VOLUME /working
WORKDIR /working

ADD docker-files/start.sh /tmp/start.sh
CMD ["bash", "/tmp/start.sh", "-d"]
