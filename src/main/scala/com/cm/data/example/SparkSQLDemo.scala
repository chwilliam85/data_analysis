package com.cm.data.example

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    val connProp = new Properties
    connProp.put("driver", "com.mysql.jdbc.Driver")
    connProp.put("user", "cmoutbigdata")
    connProp.put("password", "bd@#782018")

    val df = sql.read.jdbc("jdbc:mysql://gz-cdbro-jhhihuc1.sql.tencentcdb.com:63950/pay_chaomeng",
      "bank",
      connProp)

    df.show()

    sc.stop()
  }
}
