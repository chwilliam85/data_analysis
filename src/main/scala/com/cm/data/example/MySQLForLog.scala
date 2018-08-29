package com.cm.data.example

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MySQLForLog {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext
    val spark = SparkSession.builder.appName("MySQLForLog").getOrCreate

    val connProp = new Properties
    connProp.put("driver", "com.mysql.jdbc.Driver")
    connProp.put("user", "cmoutbigdata")
    connProp.put("password", "bd@#782018")

    val predicates = Array[String]("id<18010101900000047")

    val df = spark.read.jdbc(
      "jdbc:mysql://gz-cdbrg-nupxmf67.sql.tencentcdb.com:62382/pay_chaomeng_log",
      "order_log_20180101",
      predicates,
      connProp)

    df.show()

    spark.stop()
  }
}
