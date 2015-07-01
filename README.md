http://download.csdn.net/detail/tondayong1981/8680589  
hadoop 2.7.0 eclipse-plugin


http://mvnrepository.com/artifact/org.apache.hadoop  
hadoop maven



解压文件  
tar zxvf hadoop-2.7.0.tar.gz


sudo gedit /etc/profile  
配置hadoop环境变量

    export JAVA_HOME=/home/hadoop/jdk1.7.0_45
    export JRE_HOME=${JAVA_HOME}/jre  
    export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib  
    export PATH=${JAVA_HOME}/bin:$PATH
    

    export M2_HOME=/home/hadoop/apache-maven-3.0.5  
    export PATH=$PATH:$M2_HOME/bin 

    export  HADOOP_PREFIX=/home/hadoop/hadoop-2.7.0
    export  HADOOP_COMMON_HOME=/home/hadoop/hadoop-2.7.0
    export  HADOOP_MAPRED_HOME=/home/hadoop/hadoop-2.7.0
    export  HADOOP_CONF_DIR=/home/hadoop/hadoop-2.7.0/etc/hadoop
    export  HADOOP_HDFS_HOME=/home/hadoop/hadoop-2.7.0
    export  HADOOP_YARN_HOME=/home/hadoop/hadoop-2.7.0
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin


使用配置生效  
source /etc/profile



gedit ./etc/hadoop/hadoop-env.sh  
    export JAVA_HOME=/home/hadoop/jdk1.7.0_45


gedit ./etc/hadoop/core-site.xml  
```xml
    <configuration>
        <property>
           <name>fs.defaultFS</name>
           <value>hdfs://localhost:9000</value>
        </property>
    </configuration>
```

gedit ./etc/hadoop/hdfs-site.xml  
```xml
    <configuration>
    	<property>
    		<name>dfs.replication</name>
    		<value>1</value>
    	</property>
    </configuration>
```

cp ./etc/hadoop/mapred-site.xml.template ./etc/hadoop/mapred-site.xml  
gedit ./etc/hadoop/mapred-site.xml 
```xml
    <configuration>
    	<property>
           <name>mapreduce.framework.name</name>
           <value>yarn</value>
    </property>
    <property>
           <name>mapreduce.cluster.local.dir</name>
           <value>/home/hadoop/mapreduce/local</value>
    </property>
</configuration>
```


gedit ./etc/hadoop/yarn-site.xml
```xml
<configuration>
    <property>
       <name>yarn.resourcemanager.hostname</name>  
       <value>localhost</value>
    </property>
    <property>
       <name>yarn.nodemanager.aux-services</name> 
       <value>mapreduce_shuffle</value> 
    </property>
</configuration>
```

格式化  
    bin/hdfs namenode -format

    sbin/start-dfs.sh
    sbin/start-yarn.sh
    sbin/mr-jobhistory-daemon.sh start  historyserver

jps

    http://localhost:8088        -- 查看application 运行情况
    http://localhost:50070       -- HDFS
    http://localhost:19888       -- JobHistory


    sbin/stop-dfs.sh
    sbin/stop-yarn.sh
    sbin/mr-jobhistory-daemon.sh stop  historyserver



1、配置Mahout环境变量  

    # set mahout environment
    export MAHOUT_HOME=/home/hadoop/apache-mahout-0.10.1
    export MAHOUT_CONF_DIR=$MAHOUT_HOME/conf
    export PATH=$MAHOUT_HOME/conf:$MAHOUT_HOME/bin:$PATH

2、配置Mahout所需的Hadoop环境变量  

    # set hadoop environment
    export HADOOP_HOME=/home/yujianxin/hadoop/hadoop-1.1.2 
    export HADOOP_CONF_DIR=$HADOOP_HOME/conf 
    export PATH=$PATH:$HADOOP_HOME/bin
mahout 笔记
================================

http://download.csdn.net/detail/tondayong1981/8680589
hadoop 2.7.0 eclipse-plugin

http://mvnrepository.com/artifact/org.apache.hadoop
hadoop maven




tar zxvf hadoop-2.7.0.tar.gz


sudo gedit /etc/profile
配置hadoop环境变量
export JAVA_HOME=/home/hadoop/jdk1.7.0_45
export JRE_HOME=${JAVA_HOME}/jre  
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib  
export PATH=${JAVA_HOME}/bin:$PATH

export M2_HOME=/home/hadoop/apache-maven-3.0.5  
export PATH=$PATH:$M2_HOME/bin 

export  HADOOP_PREFIX=/home/hadoop/hadoop-2.7.0
export  HADOOP_COMMON_HOME=/home/hadoop/hadoop-2.7.0
export  HADOOP_MAPRED_HOME=/home/hadoop/hadoop-2.7.0
export  HADOOP_CONF_DIR=/home/hadoop/hadoop-2.7.0/etc/hadoop
export  HADOOP_HDFS_HOME=/home/hadoop/hadoop-2.7.0
export  HADOOP_YARN_HOME=/home/hadoop/hadoop-2.7.0
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin


source /etc/profile



gedit ./etc/hadoop/hadoop-env.sh
export JAVA_HOME=/home/hadoop/jdk1.7.0_45


gedit ./etc/hadoop/core-site.xml
<configuration>
	<property>
       <name>fs.defaultFS</name>
       <value>hdfs://localhost:9000</value>
	</property>
</configuration>


gedit ./etc/hadoop/hdfs-site.xml
<configuration>
	<property>
		<name>dfs.replication</name>
		<value>1</value>
	</property>
</configuration>


cp ./etc/hadoop/mapred-site.xml.template ./etc/hadoop/mapred-site.xml
gedit ./etc/hadoop/mapred-site.xml
<configuration>
	<property>
       <name>mapreduce.framework.name</name>
       <value>yarn</value>
</property>
<property>
       <name>mapreduce.cluster.local.dir</name>
       <value>/home/hadoop/mapreduce/local</value>
</property>
</configuration>


gedit ./etc/hadoop/yarn-site.xml
<configuration>
<property>
       <name>yarn.resourcemanager.hostname</name>  
       <value>localhost</value>
</property>
<property>
       <name>yarn.nodemanager.aux-services</name> 
       <value>mapreduce_shuffle</value> 
</property>
</configuration>


bin/hdfs namenode -format

sbin/start-dfs.sh
sbin/start-yarn.sh
sbin/mr-jobhistory-daemon.sh start  historyserver

jps

http://localhost:8088        -- 查看application 运行情况
http://localhost:50070       -- HDFS
http://localhost:19888       -- JobHistory


sbin/stop-dfs.sh
sbin/stop-yarn.sh
sbin/mr-jobhistory-daemon.sh stop  historyserver



1、配置Mahout环境变量
# set mahout environment
export MAHOUT_HOME=/home/hadoop/apache-mahout-0.10.1
export MAHOUT_CONF_DIR=$MAHOUT_HOME/conf
export PATH=$MAHOUT_HOME/conf:$MAHOUT_HOME/bin:$PATH

2、配置Mahout所需的Hadoop环境变量
# set hadoop environment
export HADOOP_HOME=/home/yujianxin/hadoop/hadoop-1.1.2 
export HADOOP_CONF_DIR=$HADOOP_HOME/conf 
export PATH=$PATH:$HADOOP_HOME/bin



