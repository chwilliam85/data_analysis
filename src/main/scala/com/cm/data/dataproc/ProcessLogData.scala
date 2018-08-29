package com.cm.data.dataproc

import org.apache.spark.sql.SparkSession

object ProcessLogData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ProcessLogData").master("local").getOrCreate()

    val userBaseDF = spark.read.parquet("hdfs://master:9000/cmdb/user_base")
    val areaCodeDF = spark.read.parquet("hdfs://master:9000/cmdb/areacode")

    userBaseDF.distinct().show(50)

//    val maxID = schemaDF.agg("id" -> "max") //从mysql读取的数据截止的ID

    spark.stop()

  }

}
