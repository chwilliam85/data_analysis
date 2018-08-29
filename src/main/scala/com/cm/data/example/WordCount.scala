package com.cm.data.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:/A.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).foreach(println)

    sc.stop()

  }
}