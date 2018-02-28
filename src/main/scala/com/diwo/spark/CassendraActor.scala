package com.diwo.spark

/*
* com.diwo.spark.CassendraActor.scala
*
* Copyright (c) 2018.  Loven Systems - All Rights Reserved
*
* Unauthorized copying of this file, via any medium is strictly
 * prohibited
*
*/
import akka.actor.{Actor, PoisonPill}
import com.diwo.util.SparkUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
/**
  * This  Actor is responisble for doing transformations
  * on dataframe which is sent by file reader actor and then
  * persists to csv file/cassendra.
  */
class CassendraActor extends Actor {

  val spark = SparkUtil.sparkconf
  var file_name : String = ""

  def receive = {

    case filename : String => {
      file_name = filename
    }

    case saveDf: DataFrame => {
      import spark.implicits._
      saveDf.createTempView("saveDftable")
      //Selecting the required columns
      val newDf = spark.sql(" select brnd_cd,sto_loc_id,shpg_trxn_id as shpg_id,shpg_trxn_ln_dt," +
        "itm_sku_num as itm_sku_id,net_sales,net_sls_qty,wt_unt_cst_amt from saveDftable")
      //Null check
      val dfwithoutNulls = newDf.na.fill(0, Seq("SRC_RGST_NBR")).na.fill("", Seq("skuid"))
      val tdf = dfwithoutNulls.toDF(dfwithoutNulls.columns map(_.toLowerCase): _*)

      //Reading Time Dimension Info
      val txtDf = spark.read.format("com.databricks.spark.csv").option("header", "true")
                  .load(Main.nrfcal_path)

      val case_tdf = txtDf.toDF(txtDf.columns map(_.toLowerCase): _*).createTempView("caseTDF")


      val transDf = spark.sql("""select TO_DATE(CAST(UNIX_TIMESTAMP(trxn_dt, 'MM/dd/yyyy') AS timestamp)) as trxn_dt ,nrf_year,nrf_week,nrf_month,nrf_season,nrf_quarter,nrf_day from caseTDF""").toDF()

      val finalDf = transDf.join(tdf,transDf.col("trxn_dt") === tdf.col("shpg_trxn_ln_dt"))

      //Reading Product Dimension Info
      val prdctDf = spark.read.format("com.databricks.spark.csv").option("header", "true")
        .load(Main.prdctinfo_path)

       val transdfPrdct = finalDf.join(prdctDf,prdctDf.col("itm_sku_num")=== finalDf.col("itm_sku_id"),"leftouter")

       //Reading Location info
       val locatnDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
        .load(Main.locatninfo_path)

       val translocatnDF = transdfPrdct.join(locatnDF,locatnDF.col("LOC_ID")=== transdfPrdct.col("sto_loc_id"),"leftouter")
        .select( "nrf_year","nrf_season","nrf_quarter","nrf_month","nrf_week","nrf_day",
          "loc_id","rgn_id","itm_sku_id","colorfamily","net_sales","net_sls_qty","wt_unt_cst_amt")

        /*  //Reading Transaction Reference Info
        val txrnDf = spark.read.format("com.databricks.spark.csv").option("header", "true")
        .option("delimiter", "|")
        .load(Main.txrn_path)

       val taxrn = transdfPrdct.join(txrnDf,txrnDf.col("shpg_trxn_id").cast("int")=== transdfPrdct.col("shpg_id").cast("int"),"leftouter" )

      //Reading Customer info
      val custmrDf = spark.read.format("com.databricks.spark.csv").option("header", "true")
        .option("delimiter", "|")
        .load(Main.custmr_path).createTempView("custmrtable")

      val custtransdf = spark.sql("select rid as cust_rid ,vs_customer_id,upd_dbase_dt from custmrtable");*/

      //val custmr = taxrn.join(custtransdf,custtransdf.col("cust_rid" )=== taxrn.col("rid"),"leftouter" ).filter($"cust_rid".isNotNull)
      SparkUtil.logger.info("Exporting final data to excel")
       translocatnDF
        .coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save(Main.outputDir_path+"/"+file_name+"basedata")

      // Persistenting data into Cassendra
      /* transdfPrdct.write.format ( "org.apache.spark.sql.cassandra" )
        .options ( Map ( "table" -> "salesnew", "keyspace" -> "persistence") )
       .mode (SaveMode.Append).save()*/

      self ! PoisonPill
    }
  }

  override def postStop {

    SparkUtil.logger.info("Stop method called")

  }
}
