README
===========================
本文件将引导读者完成大数据平台的安装、部署、测试以及运维，包括Hadoop、Zookeeper、HBase、Spark、Kafka等分布式平台。

****
	
|Author|lavaluv|
|---|---
|E-mail|huangboqi@antiy.cn


****
## 目录
* [运行环境](#运行环境)
	* 软件版本
	* SSH密匙
	* Java
* [官方文档](#官方文档)
* [CentOS](#centOS)
	* 系统配置
	* 分布式迁移
* [Hadoop](#hadoop)
    * 下载安装
    * 环境配置
    * 单机部署
    	* 运行测试
    * 伪分布式部署
    	* 配置core-site.xml
    	* 配置hdfs-site.xml
    	* 配置yarn-site.xml
    	* 配置mapred-site.xml
    	* 启动节点
    	* 运行测试
    	* 监控页面
    * 完全分布式部署
* [HBase](#hbase)
	* 下载安装
	* 环境配置
    * 单机部署
    	* 配置hbase-site.xml
    	* 启动服务
    	* hbase shell
    	* 监控页面
    * 伪分布式部署
    * 完全分布式部署
* [Zookeeper](#zookeeper) 
	* 下载安装
	* 环境配置
		* 配置文件
		* 启动节点
    * 单机部署
    * 伪分布式部署
    * 完全分布式部署
* [Spark](#spark)
	* 下载安装
	* 环境配置
    * 单机部署
        * 配置spark-env.sh
    	* spark-shell
    	* 监控页面
    * 伪分布式部署
    * 完全分布式部署
* [Kafka](#kafka)
	* 下载安装
	* 环境配置
    * 单机部署
    * 伪分布式部署
    * 完全分布式部署

运行环境
-----------

### 软件版本
```
	CentOS:CentOS-7-x86_64-DVD-1804
	Java_jre:jre-8u171-linux-x64
	Hadoop:2.7.6
	HBase:2.0.0
	Zookeeper:3.4.12
	Spark:2.3.0
	Kafka:1.1.0
```
*各平台的兼容性请参考官方文档*
### SSH密匙
检查ssh协议：
```bash
	TODO
```
安装ssh协议：
```bash
	yum install ssh
```
启动服务：
```bash
	service sshd restart
```
在Master主机上生成密匙对：
```bash
	ssh-keygen -t rsa
```
```
	rsa是加密算法,询问密码时可以选择空密码，
	如果设置密码，每次开启服务时仅需要输入一次密码；
	询问其保存路径时直接回车采用默认路径（/home/*YourUserName*/.ssh）。
```
把公钥id_rsa.pub追加到authorized_keys里:
```bash
	cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```
修改anthorized_keys的权限:
```bash
	chmod 600 ~/.ssh/authorized_keys
```
修改ssh配置文件:
```bash
	vi /etc/ssh/sshd_config
```
```
	把文件中的下面几条信息的注释去掉：　
　　    RSAAuthentication yes # 启用 RSA 认证
　　    PubkeyAuthentication yes # 启用公钥私钥配对认证方式
　　    AuthorizedKeysFile .ssh/authorized_keys # 公钥文件路径（和上面生成的文件同）
```
重启服务:
```bash
	service sshd restart
```
分布式部署时，需要将公匙复制到slave机器上:
```bash
	scp ~/.ssh/id_rsa.pub YourSlaveName@YourSlaveIP:~/
```
### Java 
`JRE下载地址`:[http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html](http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html)  

或直接使用yum安装jdk:
```bash
	yum install java
```
修改bashrc环境变量:
```bash
	vi ~/.bashrc
```
输入：
```bash
	export JAVA_HOME=/your/java/home
	export PATH=$PATH:$JAVA_HOME/bin
```
键入'ESC'退出编辑，输入':wq'保存并退出  
使环境变量生效:
```bash
	source ~/.bashrc
```

--[返回目录](#目录)--

官方文档
------
`Hadoop官方文档`：[http://hadoop.apache.org/docs/r2.7.6/](http://hadoop.apache.org/docs/r2.7.6/)  
`HBase官方文档`：[http://hbase.apache.org/book.html](http://hbase.apache.org/book.html)  
`Zookeeper官方文档`：[http://zookeeper.apache.org/doc/r3.4.12/](http://zookeeper.apache.org/doc/r3.4.12/)  
`Spark官方文档`：[http://spark.apache.org/docs/latest/](http://spark.apache.org/docs/latest/)  
`Kafka官方文档`：[http://kafka.apache.org/documentation/](http://kafka.apache.org/documentation/)

CentOS
------
### 系统配置
`设置VMware端口转换`:
[![vmNAT]]()  
`使用VMware加载centOS镜像，尽量分配较多的内存以及处理器资源`:  
[![vm]]()  
`配置centOS图形化界面`:
[![centOSUI]]()  
`配置用户`:
[![centOSUSER]]()

### 分布式迁移

--[返回目录](#目录)--

Hadoop
------

### 下载安装
`下载地址`:[http://hadoop.apache.org/releases.html](http://hadoop.apache.org/releases.html)  

解压并移动到自己创建的目录下:
```bash
	tar -zxf /your/hadoop/tar -C /your/destination
```
### 环境配置
配置hadoop环境变量:
```bash
	vi ~/.bashrc
```
键入:
```
	export HADOOP_HOME=/your/hadoop/dir
	export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```
使配置生效:
```bash
	source ~/.bashrc
```

### 单机部署
#### 运行测试
创建wordcount输入文件：
```bash
	cat /home/UserName/Desktop/wc.input
	hadoop marpreduce hive
	hbase spark storm'
	sqoop hadoop hive
	spark hadoop
```
运行hadoop自带的wordcount例子:
```bash
	hadoop jar hadoop-mapreduce-examples-2.7.6.jar wordcount /home/UserName/Desktop/wc.input
```
### 伪分布式部署
#### 配置core-site.xml
`hadoop配置文件在hadoop目录下的etc/hadoop里`  
在core-site.xml里添加:
```xml
	<configuration>
   		<property>
      		<name>fs.default.name </name>
      		<value> hdfs://localhost:8020 </value> 
   		</property>
	</configuration>
```
其中hdfs://localhost:8020代表运行在本地模式
#### 配置hdfs-site.xml
在hdfs-site.xml里添加:
```xml
<configuration>

   <property>
      <name>dfs.replication</name>
      <value>1</value>
   </property>
    
   <property>
      <name>dfs.name.dir</name>
      <value>file:///home/hadoop/hadoopinfra/hdfs/namenode </value>
   </property>
    
   <property>
      <name>dfs.data.dir</name> 
      <value>file:///home/hadoop/hadoopinfra/hdfs/datanode </value> 
   </property>
       
</configuration>
```
其中dfs.replication为设置副本数量，  
dfs.name.dir为设置namenode的存储地址，  
dfs.data.dir为datanode的存储地址
#### 配置yarn-site.xml
在yarn-site.xml里添加:
```xml
<configuration>
 
   <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value> 
   </property>
  
</configuration>
```
#### 配置mapred-site.xml
将mapred-site.xml.template重命名为mapred-site.xml并输入：
```xml
<configuration>
 
   <property> 
      <name>mapreduce.framework.name</name>
      <value>yarn</value>
   </property>
   
</configuration>
```
#### 启动节点
格式化HDFS：
```bash
	hdfs namenode -format
```
启动namenode,datanode,secondary namenode:
```bash
	start-dfs.sh
```
启动yarn:
```bash
	start-yarn.sh
```
关闭服务:
```bash
	stop-all.sh
```
#### 运行测试
```bash
	hadoop jar hadoop-mapreduce-examples-2.7.6.jar wordcount /home/UserName/Desktop/wc.input
```
#### 监控页面
访问Hadoop默认端口号为8088，使用以下网址获得浏览器Hadoop的服务:
```
	http://localhost:8088
```
### 完全分布式部署

--[返回目录](#目录)--

HBase
------

### 下载安装
`下载地址`:[http://hbase.apache.org/downloads.html](http://hbase.apache.org/downloads.html)   

解压并移动到自己创建的目录下:
```bash
	tar -zxf /your/hbase/tar -C /your/destination
```
### 环境配置
配置hadoop环境变量:
```bash
	vi ~/.bashrc
```
键入:
```
	export HBASE_HOME=/your/hbase/dir
	export PATH=$HBASE_HOME/bin:$HBASE_HOME/sbin:$PATH
```
使配置生效:
```bash
	source ~/.bashrc
```
### 单机部署
`单机模式下HBase运行在本地，不需要HDFS支持`
#### 配置hbase-site.xml
配置文件在hbase目录/conf下，修改添加:
```xml
	<configuration>
		<property>
			<name>hbase.rootdir</name>
			<value>file:///your/dir/name</value>
		</property>
	<configuration>
```
#### 启动服务
```bash
	start-hbase.sh
```
使用jps查看进程。  
停止服务:
```bash
	stop-hbase.sh
```
#### hbase shell
进入hbase shell:
```bash
	hbase shell
```
hbase shell基本操作：
```bash
	help   ##显示帮助
	create 'tableName','columnFamilyName',...	##创建表
	scan 'tableName'	##扫描表	
	get 'tableName','columnFamilyName'	##获取某行数据
```
#### 监控页面
访问HBase默认端口号为60010，使用以下网址获得浏览器Hbase的服务:
```
	http://localhost:60010
```
### 伪分布式部署
`伪分布式的HBase依赖于HDFS,需要启动hadoop服务`

### 完全分布式部署

--[返回目录](#目录)--

Zookeeper
------

### 下载安装
`下载地址`:[http://zookeeper.apache.org/releases.html#download](http://zookeeper.apache.org/releases.html#download)  

解压并移动到自己创建的目录下:
```bash
	tar -zxf /your/zookeeper/tar -C /your/destination
```
### 环境配置
配置hadoop环境变量:
```bash
	vi ~/.bashrc
```
键入:
```
	export ZOOKEEPER_HOME=/your/zookeeper/dir
	export PATH=$HBASE_HOME/bin:$ZOOKEEPER_HOME/sbin:$PATH
```
使配置生效:
```bash
	source ~/.bashrc
```

### 单机部署
#### 配置文件
切换到Zookeeper目录/conf下，修改zoo.cfg文件:
```bash
	tickTime=2000
	dataDir=/your/zookeeper/dir/data
	dataLogDir=/your/zookeeper/dir/logs
	clientPort=2181
```
#### 启动节点
节点启动:
```bash
	./zkServer.sh start
```
节点关闭:
```bash
	./zkServer.sh stop
```
### 伪分布式部署

### 完全分布式部署

--[返回目录](#目录)--

Spark
------

### 下载安装
`下载地址`:[http://spark.apache.org/downloads.html](http://spark.apache.org/downloads.html)  

解压并移动到自己创建的目录下:
```bash
	tar -zxf /your/spark/tar -C /your/destination
```
### 环境配置
配置hadoop环境变量:
```bash
	vi ~/.bashrc
```
键入:
```
	export ZOOKEEPER_HOME=/your/spark/dir
	export PATH=$HBASE_HOME/bin:$SPARK_HOME/sbin:$PATH
```
使配置生效:
```bash
	source ~/.bashrc
```

### 单机部署

### 伪分布式部署

### 完全分布式部署

--[返回目录](#目录)--

Kafka
------

### 下载安装
`下载地址`:[http://kafka.apache.org/downloads](http://kafka.apache.org/downloads) 

解压并移动到自己创建的目录下:
```bash
	tar -zxf /your/kafka/tar -C /your/destination
```
### 环境配置
配置hadoop环境变量:
```bash
	vi ~/.bashrc
```
键入:
```
	export KAFKA_HOME=/your/zookeeper/dir
	export PATH=$HBASE_HOME/bin:$KAFKA_HOME/sbin:$PATH
```
使配置生效:
```bash
	source ~/.bashrc
```

### 单机部署

### 伪分布式部署

### 完全分布式部署

--[返回目录](#目录)--

--------------------------------
