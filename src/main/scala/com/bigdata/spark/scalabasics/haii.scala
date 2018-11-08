package com.bigdata.spark.scalabasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object haii {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("haii").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("haii").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("haii").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data = args(0)
    val op = args(1)
    val rdd = sc.textFile(data)
    rdd.collect.foreach(println)
    rdd.saveAsTextFile(op)

   // val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)

   // df.show()
   // df.write.format("csv").option("header","true").save(op)

    spark.stop()
  }
}