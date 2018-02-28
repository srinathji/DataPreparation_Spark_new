package com.diwo.util

/*
* SparkUtil.scala
*
* Copyright (c) 2018.  Loven Systems - All Rights Reserved
*
* Unauthorized copying of this file, via any medium is strictly
 * prohibited
*
*/
import com.diwo.spark.Main
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
/**
  * This is the Configuration file for the Spark job
  * The app contains all configuration and performance related
  * settings.
  */
object SparkUtil {

def sparkconf = {

    val conf = new SparkConf()
   //.setMaster("local[2]")
   .setAppName("SparkApplication")
   .set("spark.driver.allowMultipleContexts","true")
   .set("spark.executor.memory", "6g")
   .set("spark.cassandra.connection.host",Main.cassendra_ipaddr.toString)
   .set("spark.sql.warehouse.dir", Main.warehouse_path.toString)

  //Optimization settings
  conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.debug","maxToStringFields")
  conf.set("spark.sql.crossJoin.enabled", "true")
  conf.set("spark.kryoserializer.buffer.max","768m")
  conf.set("spark.sql.codegen","true")
  conf.set("spark.debug.maxToStringFields","100000")
  //log4j


 val spark = SparkSession.builder()
   //.config("spark.sql.warehouse.dir", "Warehouse path")
   .config(conf)
   .getOrCreate()
 spark

 }



  def remove_leading: String => String = _.replaceAll("[,.\\s]", "")

  val udf_remove = udf(remove_leading)
  val logger = Logger.getLogger("Dataprep_spark")

}
