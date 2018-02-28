package com.diwo.spark

/*
* Main.scala
*
* Copyright (c) 2018.  Loven Systems - All Rights Reserved
*
* Unauthorized copying of this file, via any medium is strictly
 * prohibited
*
*/

import akka.actor.{ActorSystem, Props}
import org.apache.log4j.{Level, Logger}
/**
  * This is the main app for the Spark job
  * The app creates the Actor system and sends message
  * to File Reader actor.
  */
object Main extends App {

  if( args.length < 8) {

    System.err.println("Help message: use Jar spark_warehouse_path  cassendraipaddr file://rawdatafilepath file://nrfcalendarpath  file://productinfopath " +
      " outputdir Hit-ENTER")

  }
  val warehouse_path = args(0)
  //Cassendra Ip address
  val cassendra_ipaddr = args(1)
  //Raw data file argument
  val rawdata_path = args(2)
  //NRFCalendar argument
  val nrfcal_path = args(3)
  /*File with headers or No headers
  boolean argument
  true-with headers
  false-No headers
  */
  val headers_flag = args(4).toString
  //Product info file argument
  val prdctinfo_path = args(5)
  //Location file argument
  val locatninfo_path = args(6)

  //Output file path
  val outputDir_path = args(7)
  //Customer Info Args
  //val txrn_path = args(6)
  // val custmr_path = args(7)

  Logger.getLogger ( "org" ).setLevel ( Level.OFF )
  Logger.getLogger ( "akka" ).setLevel ( Level.OFF )

  val actorsystem  = ActorSystem("FileReader")
  val actor = actorsystem.actorOf(Props[FileReader])
  val future = actor ! "Hello";

}
