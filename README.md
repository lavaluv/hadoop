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
* [CentOS](#CentOS)
* [Hadoop](#Hadoop)
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
    	* Java API
    	* 监控页面
    * 完全分布式部署
* [HBase](#HBase)
	* 下载安装
	* 环境配置
    * 单机部署
    	* 配置hbase-site.xml
    	* 启动服务
    	* hbase shell
    	* java API
    	* 监控页面
    * 伪分布式部署
    * 完全分布式部署
* [Zookeeper](#Zookeeper) 
	* 下载安装
	* 环境配置
    * 单机部署
    * 伪分布式部署
    * 完全分布式部署
* [Spark](#Spark)
	* 下载安装
	* 环境配置
    * 单机部署
        * 配置spark-env.sh
    	* spark-shell
    	* Java API
    	* 监控页面
    * 伪分布式部署
    * 完全分布式部署
* [Kafka](#Kafka)
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



官方文档
------
`Hadoop官方文档`：[http://hadoop.apache.org/docs/r2.7.6/](http://hadoop.apache.org/docs/r2.7.6/)  
`HBase官方文档`：[http://hbase.apache.org/book.html](http://hbase.apache.org/book.html)  
`Zookeeper官方文档`：[http://zookeeper.apache.org/doc/r3.4.12/](http://zookeeper.apache.org/doc/r3.4.12/)  
`Spark官方文档`：[http://spark.apache.org/docs/latest/](http://spark.apache.org/docs/latest/)  
`Kafka官方文档`：[http://kafka.apache.org/documentation/](http://kafka.apache.org/documentation/)

CentOS
------


Hadoop
------

### 下载安装

### 环境配置

### 单机部署
#### 运行测试

### 伪分布式部署
#### 配置core-site.xml
#### 配置hdfs-site.xml
#### 配置yarn-site.xml
#### 配置mapred-site.xml
#### 启动节点
#### 运行测试
#### Java API
#### 监控页面

### 完全分布式部署

HBase
------

### 下载安装

### 环境配置

### 单机部署
#### 配置hbase-site.xml
#### 启动服务
#### hbase shell
#### Java API
#### 监控页面

### 伪分布式部署

### 完全分布式部署

Zookeeper
------

### 下载安装

### 环境配置

### 单机部署

### 伪分布式部署

### 完全分布式部署

Spark
------

### 下载安装

### 环境配置

### 单机部署

### 伪分布式部署

### 完全分布式部署

Kafka
------

### 下载安装

### 环境配置

### 单机部署

### 伪分布式部署

### 完全分布式部署

--------------------------------
