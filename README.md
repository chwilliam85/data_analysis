# 大数据分析平台

#### 项目介绍
大数据分析平台

#### 软件架构
软件架构说明


#### 安装教程

1. xxxx
2. xxxx
3. xxxx

#### 使用说明

本地模式，执行SparkSQLDemo：
spark-submit --packages "mysql:mysql-connector-java:6.0.6" \
--class "com.cm.data.example.MySQLForLog" --master local[4] target/data_analysis-1.0.0.jar

YARN客户端模式，执行SparkSQLDemo：
spark-submit --packages "mysql:mysql-connector-java:6.0.6" \
--class "com.cm.data.example.MySQLForLog" --master yarn --deploy-mode client  target/data_analysis-1.0.0.jar

拉取订单表数据 YARN集群模式执行ReadLogDb2HDFS（predicates字段）：
spark-submit --master yarn --deploy-mode cluster --packages "mysql:mysql-connector-java:6.0.6" --num-executors 4 --executor-memory 4G --class "com.cm.data.datasync.ReadLogDb2HDFS" target/data_analysis-1.0.1.jar order_log_20180101 pay_type 0101000000,0101115959,0101120000,0101155959,0101160000,0101185959,0101190000,0101235959

拉取订单表数据 YARN集群模式执行ReadLogDb2HDFS（指定临界值）：
spark-submit --master yarn --deploy-mode cluster --packages "mysql:mysql-connector-java:6.0.6" --num-executors 4 --executor-memory 4G --class "com.cm.data.datasync.ReadLogDb2HDFS" target/data_analysis-1.0.1.jar order_log_20180101 date 4
