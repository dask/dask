FROM ubuntu:trusty

# conda
RUN apt-get update && apt-get install -y -q curl bzip2 git
RUN curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

# hdfs3 - python
RUN apt-get install -y -q protobuf-compiler libprotobuf-dev
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
RUN /opt/conda/bin/conda install -y -q libxml2 krb5 boost uuid ipython pytest pip pandas
RUN /opt/conda/bin/conda install -y -q libhdfs3 libgsasl libntlm -c blaze
RUN /opt/conda/bin/pip install git+https://github.com/blaze/hdfs3
RUN /opt/conda/bin/pip install git+https://github.com/blaze/dask --upgrade


# Cloudera repositories
RUN curl -s http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key | apt-key add -
RUN echo 'deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' > /etc/apt/sources.list.d/cloudera.list
RUN echo 'deb-src http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' >> /etc/apt/sources.list.d/cloudera.list
ADD docker-files/cloudera.pref /etc/apt/preferences.d/cloudera.pref

# Install CDH5 in a single node: Pseudo Distributed
ADD docker-files/cdh5-install.sh /tmp/cdh5-install.sh
RUN bash /tmp/cdh5-install.sh

EXPOSE 8020
EXPOSE 50070

VOLUME /distributed
WORKDIR /distributed

ADD docker-files/start.sh /tmp/start.sh
CMD ["bash", "/tmp/start.sh"]
