package com.cm.data.datasync

import com.cm.data.sql.CMOrderLog
import com.cm.data.util.{DBHelper, StringUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 处理订单日志表
  */
object ReadLogDb2HDFS {

  private val OUTPUT = "hdfs://master:9000/cmdb/"
  final private val URL = "jdbc:mysql://gz-cdbrg-nupxmf67.sql.tencentcdb.com:62382/pay_chaomeng_log"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Load Log Data").getOrCreate

    getOrderByRange(spark, args)
  }

  /**
    * 调用 jdbc(url, tableName, partitionName, start, end, partitionNum, connProp)
    *
    * @param spark sparkSession
    * @param args  运行参数
    */
  def getOrderByRange(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val idRange = CMOrderLog.getIDRange(tableName, URL)

    //分区数
    val partitionNum = args(1).toInt

    //ID值起始范围
    val start = idRange.get(0)
    val end = idRange.get(1)

    val dir_1 = tableName.substring(0, tableName.lastIndexOf("_"))
    val dir_2 = tableName.substring(tableName.lastIndexOf("_") + 1)

    val df = spark.read.jdbc(URL, tableName, "id", start, end, partitionNum, DBHelper.setConnectionProperty)

    df.write.mode(SaveMode.Overwrite).parquet(OUTPUT + dir_1 + "/date=" + dir_2)

    spark.stop()
  }

  /**
    * 调用 jdbc(url, tableName, predicates, connProp) 方法
    *
    * @param spark sparkSession
    * @param args  运行参数
    */
  def getOrderByPredicate(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    //分区字段
    val partitionName = args(1)

    //时间戳（支持乱序） "0101235959,0101115959,0101120000,0101185959,0101160000,0101155959,0101190000,0101000000"
    val date = args(2)

    val dir_1 = tableName.substring(0, tableName.lastIndexOf("_"))
    val dir_2 = tableName.substring(tableName.lastIndexOf("_") + 1)

    val predicates = StringUtils.dynamicPartitions(date, "createtime")

    val df = spark.read.jdbc(URL, tableName, predicates, DBHelper.setConnectionProperty)

    if (StringUtils.isNotEmpty(partitionName)) {
      df.write.partitionBy(partitionName).mode(SaveMode.Overwrite).parquet(OUTPUT + dir_1 + "/date=" + dir_2)
    } else {
      df.write.mode(SaveMode.Overwrite).parquet(OUTPUT + dir_1 + "/date=" + dir_2)
    }

    spark.stop()
  }

}
