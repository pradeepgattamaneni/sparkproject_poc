package com.bigdata.spark.sparkcore.travel

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object high_air_revenue {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("high_air_revenue").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("high_air_revenue").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("high_air_revenue").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val travelrdd = sc.textFile("C:\\work\\impfiles\\data\\TravelData.txt")
    val fil = travelrdd.map(x=>x.split('\t')).map(x=>(x(2),x(3).toInt)).filter(x=> x._2==1)
    //val fil = travelrdd.map(x=>x.split('\t')).filter(x=>{if(x(3).matches("1")) true else false })
    val res = fil.map(x=>(x._1,1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false)
    res.take(10).foreach(println)

    spark.stop()
  }
}