# Install CDH5 in a single node: Pseudo Distributed
# Docs: http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cdh_qs_yarn_pseudo.html

apt-get update && apt-get install -y -q openjdk-7-jre-headless hadoop-conf-pseudo

# Step 1: Format the NameNode
sudo -u hdfs hdfs namenode -format -force

# Step 2: Start HDFS
for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done

# Step 3: Create the directories needed for Hadoop processes
bash /usr/lib/hadoop/libexec/init-hdfs.sh

# Step 6: Create User Directories
# sudo -u hdfs hdfs dfs -mkdir -p /user/hadoop
# sudo -u hdfs hdfs dfs -chown hadoop /user/hadoop
