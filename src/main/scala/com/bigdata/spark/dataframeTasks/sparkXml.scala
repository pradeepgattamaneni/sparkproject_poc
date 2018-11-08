package com.bigdata.spark.dataframeTasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkXml {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkXml").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkXml").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkXml").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data ="C:\\work\\impfiles\\data\\complexxmldata.xml"
    val df =spark.read.format("xml").option("rowTag","catalog_item").load(data)
    df.createOrReplaceTempView("tabs")
    val res = spark.sql("")
    // df.printSchema()

    spark.stop()
  }
}