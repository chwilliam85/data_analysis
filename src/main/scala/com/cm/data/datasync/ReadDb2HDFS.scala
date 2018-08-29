package com.cm.data.datasync

import com.cm.data.sql.CMOrderLog
import com.cm.data.util.{DBHelper, StringUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 处理一般数据量表
  */
object ReadDb2HDFS {

  private val OUTPUT = "hdfs://master:9000/cmdb/"
  final private val URL = "jdbc:mysql://gz-cdbro-jhhihuc1.sql.tencentcdb.com:63950/pay_chaomeng?useUnicode=true&amp&characterEncoding=UTF-8&amp&zeroDateTimeBehavior=convertToNull&amp&transformedBitIsBoolean=true&serverTimezone=Asia/Shanghai&useSSL=true"

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("Load Data").getOrCreate

    if (StringUtils.isNotEmpty(args(0))) {
      if (args(0).contains("shop")) {
        loadMerchantData(session, args)
      }

      if (args(0).equals("user_auth")) {
        loadUserAuthData(session, args)
      }

      if (args(0).equals("agency")) {
        loadAgencyData(session, args)
      }

      if (args(0).equals("area_agency")) {
        loadAreaAgencyData(session, args)
      }

      if (args(0).equals("user_verify_data")) {
        loadUserVerifyData(session, args)
      }

      if (args(0).equals("cashier_relation")) {
        loadCashierRelationData(session, args)
      }

      if (args(0).equals("user_bank")) {
        loadUserBankData(session, args)
      }

      if (args(0).equals("role_relation")) {
        loadRoleRelationData(session, args)
      }

      if (args(0).equals("user_base")) {
        loadUserBaseData(session, args)
      }

      if (args(0).equals("subscribe_relation")) {
        loadDataByRange(session, args)
      }

      if (args(0).equals("user_code") || args(0).equals("user2agency") || args(0).equals("agency_relation") || args(0).equals("bank_code_sup") || args(0).equals("areacode")) {
        loadData(session, args)
      }
    }
  }

  def loadMerchantData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_name", (str: String) => StringUtils.encrypt(str, 1))
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_name(t.realname) as real_name, e_name(t.bank_kh_name) as hold_name, e_phone(t.phone) as cell_phone, e_card(t.bank_card_id) as bank_card FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("realname", "phone", "bank_card_id", "bank_kh_name")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadUserAuthData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_name", (str: String) => StringUtils.encrypt(str, 1))
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_name(t.realname) as real_name, e_phone(t.mobile) as phone, e_card(t.person_card) as id_card FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("realname", "mobile", "person_card")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadAgencyData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_name", (str: String) => StringUtils.encrypt(str, 1))
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_name(t.realname) as real_name, e_phone(t.mobile) as phone, e_card(t.bank_card_num) as bank_card FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("realname", "mobile", "bank_card_num")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadAreaAgencyData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_phone(t.mobile) as phone, e_card(t.idcard) as id_card, e_card(t.bank_card_num) as bank_card FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("idcard", "mobile", "bank_card_num")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadUserVerifyData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    //ID值起始范围
    val start = args(1).toInt
    val end = args(2).toInt
    val ids = StringUtils.autoExpansion(start, end, 100000)

    val predicates = StringUtils.dynamicPartitions(ids, "id")

    val originDF = spark.read.jdbc(URL, tableName, predicates, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_name", (str: String) => StringUtils.encrypt(str, 1))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_name(t.auth_realname) as real_name, e_card(t.auth_person_card) as id_card FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("auth_realname", "auth_person_card")

    if (StringUtils.isNotEmpty(args(3))) {
      finalDF.write.partitionBy(args(3)).mode(SaveMode.Append).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Append).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadCashierRelationData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_phone(t.mobile) as phone, e_card(t.cash_code) as cash_point FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("mobile", "cash_code")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadUserBankData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_name", (str: String) => StringUtils.encrypt(str, 1))
    context.udf.register("e_card", (str: String) => StringUtils.encrypt(str))

    val sql = "SELECT t.*, e_name(t.bank_kh_name) as hold_name, e_card(t.bank_card_id) as bank_card FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("bank_kh_name", "bank_card_id")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadRoleRelationData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val originDF = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))

    val sql = "SELECT t.*, e_phone(t.mobile) as phone FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("mobile")

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      finalDF.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadUserBaseData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    //ID值起始范围
    val start = args(1).toInt
    val end = args(2).toInt
    val ids = StringUtils.autoExpansion(start, end, 2000000)

    val predicates = StringUtils.dynamicPartitions(ids, "id")

    val originDF = spark.read.jdbc(URL, tableName, predicates, DBHelper.setConnectionProperty)

    originDF.createOrReplaceTempView(tableName)

    val context = originDF.sqlContext
    context.udf.register("e_phone", (str: String) => StringUtils.encrypt(str, 3))

    val sql = "SELECT t.*, e_phone(t.mobile) as phone FROM " + tableName + " t"

    val finalDF = context.sql(sql).drop("mobile")

    if (StringUtils.isNotEmpty(args(3))) {
      finalDF.write.partitionBy(args(3)).mode(SaveMode.Append).parquet(OUTPUT + tableName)
    } else {
      finalDF.write.mode(SaveMode.Append).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadData(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val df = spark.read.jdbc(URL, tableName, DBHelper.setConnectionProperty)

    if (args.length > 1 && StringUtils.isNotEmpty(args(1))) {
      df.write.partitionBy(args(1)).mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    } else {
      df.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)
    }

    spark.stop()
  }

  def loadDataByRange(spark: SparkSession, args: Array[String]): Unit = {
    val tableName = args(0)

    val idRange = CMOrderLog.getIDRange(tableName, URL)

    //分区数
    val partitionNum = args(1).toInt

    //ID值起始范围
    val start = idRange.get(0)
    val end = idRange.get(1)

    val df = spark.read.jdbc(URL, tableName, "id", start, end, partitionNum, DBHelper.setConnectionProperty)
    df.write.mode(SaveMode.Overwrite).parquet(OUTPUT + tableName)

    spark.stop()
  }

}
