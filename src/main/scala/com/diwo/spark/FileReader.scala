package com.diwo.spark
/*
* com.diwo.spark.FileReader.scala
*
* Copyright (c) 2018.  Loven Systems - All Rights Reserved
*
* Unauthorized copying of this file, via any medium is strictly
 * prohibited
*
*/
import akka.actor.{Actor, ActorSystem, Props}
import com.diwo.util.SparkUtil
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}
/**
  * This Actor reads the data from csv file with
  * option having headers/not headers and persists the data
  * to cassendra and sends dataframe to Cassendra actor
  */

class FileReader extends Actor {

  def receive = {

    case test:String => spark_load

  }
  def spark_load  {

    val spark = SparkUtil.sparkconf
    import spark.implicits._
    var file = new java.io.File(Main.rawdata_path.toString)
    var filename = file.getName().split("\\.")(0)


    if(Main.headers_flag.equals("true")) {

      SparkUtil.logger.info("Reading csv/txt file with headers")

      //Reading raw data from csv file
      val salesdf_head = spark.read.format("com.databricks.spark.csv").option("header", "true")
        .option("customSchema", "true")
        .option("delimiter", ",")
        .load(file.toString)

      val tdf_case = salesdf_head.toDF(salesdf_head.columns map (_.toLowerCase): _*).withColumn("shpg_trxn_id", SparkUtil.udf_remove($"shpg_trxn_id"))

      val transDF = tdf_case.na.fill("", Seq("trxn_bgn_tm")).na.fill("", Seq("itm_sku_num"))

      //Saving Raw data into cassendra
      SparkUtil.logger.info("Persisting Raw data to Cassendra")
      transDF.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "sales", "keyspace" -> "persistence"))
        .mode(SaveMode.Append).save()

      //Creating actor and sending to dataframe to Cassendra Actor
      val actorsystem = ActorSystem("CassendraActor")
      val actor = actorsystem.actorOf(Props[CassendraActor])
      actor ! filename
      actor ! transDF

    } else {

      SparkUtil.logger.info("Reading csv/txt file with No headers")
      //val salesdf_Nohead = spark.sparkContext.textFile(Main.rawdata_path).map(x=>x.replace( "\"", "" ).split(",")).map(col => RawDataSchema(col(0).toString,
      val salesdf_Nohead = spark.read.csv(file.toString)
      val salesdf_Nohead1 = salesdf_Nohead.withColumnRenamed("_c0","BRND_CD")
      val salesdf_Nohead2 = salesdf_Nohead1.withColumnRenamed("_c1","STO_LOC_ID")
      val salesdf_Nohead3 = salesdf_Nohead2.withColumnRenamed("_c2","SHPG_TRXN_ID")
      val salesdf_Nohead4 = salesdf_Nohead3.withColumnRenamed("_c3","SHPG_TRXN_LN_DT")
      val salesdf_Nohead5 = salesdf_Nohead4.withColumnRenamed("_c4","TRXN_BGN_TM")
      val salesdf_Nohead6 = salesdf_Nohead5.withColumnRenamed("_c5","SRC_RGST_NBR")
      val salesdf_Nohead7 = salesdf_Nohead6.withColumnRenamed("_c6","ITM_SKU_NUM")
      val salesdf_Nohead8 = salesdf_Nohead7.withColumnRenamed("_c7","SRC_SLS_PRSN_NUM")
      val salesdf_Nohead9 = salesdf_Nohead8.withColumnRenamed("_c8","SRC_SLS_CSHR_NUM")
      val salesdf_Nohead10 = salesdf_Nohead9.withColumnRenamed("_c9","FP_SLS_AMT")
      val salesdf_Nohead11 = salesdf_Nohead10.withColumnRenamed("_c10","FP_SLS_QTY")
      val salesdf_Nohead12 = salesdf_Nohead11.withColumnRenamed("_c11","CLR_SLS_AMT")
      val salesdf_Nohead13 = salesdf_Nohead12.withColumnRenamed("_c12","CLR_SLS_QTY")
      val salesdf_Nohead14 = salesdf_Nohead13.withColumnRenamed("_c13","MD_SLS_AMT")
      val salesdf_Nohead15 = salesdf_Nohead14.withColumnRenamed("_c14","MD_SLS_QTY")
      val salesdf_Nohead16 = salesdf_Nohead15.withColumnRenamed("_c15","GVWY_SLS_AMT")
      val salesdf_Nohead17 = salesdf_Nohead16.withColumnRenamed("_c16","GVWY_SLS_QTY")
      val salesdf_Nohead18 = salesdf_Nohead17.withColumnRenamed("_c17","NET_SALES")
      val salesdf_Nohead19 =  salesdf_Nohead18.withColumnRenamed("_c18","NET_SLS_QTY")
      val salesdf_Nohead20 = salesdf_Nohead19.withColumnRenamed("_c19","RETURN_SALES")
      val salesdf_Nohead21 = salesdf_Nohead20.withColumnRenamed("_c20","RETURN_QTY")
      val salesdf_Nohead22 = salesdf_Nohead21.withColumnRenamed("_c21","ANGEL_TRXN")
      val salesdf_Nohead23 = salesdf_Nohead22.withColumnRenamed("_c22","WT_UNT_CST_AMT")
      val salesdf_Nohead24 = salesdf_Nohead23.withColumnRenamed("_c23","CUR_TKT_RTL_UNT_AMT")


      val tdf_case = salesdf_Nohead24.toDF(salesdf_Nohead24.columns map (_.toLowerCase): _*).withColumn("shpg_trxn_id", SparkUtil.udf_remove($"shpg_trxn_id"))
      val transDF = tdf_case.na.fill("", Seq("trxn_bgn_tm")).na.fill("", Seq("itm_sku_num"))

      //Saving Raw data into cassendra
      SparkUtil.logger.info("Persisting Raw data to Cassendra")
      transDF.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "sales", "keyspace" -> "persistence"))
        .mode(SaveMode.Append).save()

      //Creating actor and sending to dataframe to Cassendra Actor
      val actorsystem = ActorSystem("CassendraActor")
      val actor = actorsystem.actorOf(Props[CassendraActor])

      actor ! filename
      actor ! transDF

    }

  }
}
