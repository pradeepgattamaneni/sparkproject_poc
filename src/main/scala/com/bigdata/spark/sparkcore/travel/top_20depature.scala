package com.bigdata.spark.sparkcore.travel

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object top_20depature {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("top_20depature").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("top_20depature").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("top_20depature").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val travelrdd = sc.textFile("C:\\work\\impfiles\\data\\TravelData.txt")
    val res = travelrdd.map(x=>x.split("\t")).map(x=>(x(1),1)).reduceByKey(_+_).map(item=>item.swap).sortByKey(false)
    res.take(20).foreach(println)

    spark.stop()
  }
}