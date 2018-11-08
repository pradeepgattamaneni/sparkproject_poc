package com.bigdata.spark.dataframeTasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object header {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("header").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("header").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("header").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    //val data = "file:///C:\\work\\impfiles\\data\\10000Records.csv"
    //val output = "C:\\work\\impfiles\\data\\output\\1000Recordscsv"
    val data = args(0)
    val output = args(1)
    val df= spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.show()
    val cols = df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))
    val ndf =df.toDF(cols:_*)
    ndf.show(10)
    ndf.write.format("csv").option("header","true").option("delimiter","|").save(output)

    spark.stop()
  }
}