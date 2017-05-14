#this is to reset the hadoop&spark configuration file whenever my cluster is restarted
#request for new cluster dns:
echo Master=?
read master
echo Worker1=?
read worker1
echo Worker2=?
read worker2

#all notes need:
sed -i -e "2s/\(HostName \).*/\1$master/" ~/.ssh/config
sed -i -e "6s/\(HostName \).*/\1$worker1/" ~/.ssh/config
sed -i -e "10s/\(HostName \).*/\1$worker2/" ~/.ssh/config

sed -i -e "22s/\(<value>hdfs:\/\/\).*/\1$master\:9000<\/value\>/" $HADOOP_CONF_DIR/core-site.xml
sed -i -e "29s/\(<value>\).*/\1$master\<\/value\>/" $HADOOP_CONF_DIR/yarn-site.xml
sed -i -e "22s/\(<value>\).*/\1$master\:54311<\/value\>/" $HADOOP_CONF_DIR/mapred-site.xml

sed -i -e "24s/\".*\"/\"$master\"/" $SPARK_HOME/conf/spark-env.sh

#master only
sudo sed -i -e "s/.*ip-172-31-25-255/$master ip-172-31-25-255/" /etc/hosts
sudo sed -i -e "s/.*ip-172-31-18-149/$worker1 ip-172-31-18-149/" /etc/hosts
sudo sed -i -e "s/.*ip-172-31-24-4/$worker2 ip-172-31-24-4/" /etc/hosts

sed -i -e "1s/.*/$worker1/" $SPARK_HOME/conf/slaves
sed -i -e "2s/.*/$worker2/" $SPARK_HOME/conf/slaves
