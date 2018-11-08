package com.bigdata.spark.sparkcore.travel

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object popular_destination {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("popular_destination").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("popular_destination").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("popular_destination").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val travelrdd = sc.textFile("C:\\work\\impfiles\\data\\TravelData.txt")
    //val fil = textFile.map(x=>x.split('\t')).filter(x=>{if((x(3).matches(("1")))) true else false })
    //val cnt = fil.map(x=>(x(2),1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false).take(20)
    val result = travelrdd.map(x=>x.split("\t")).map(x=>(x(2),1)).reduceByKey(_+_).map(item=>item.swap).sortByKey(false)
    result.collect().foreach(println)
    spark.stop()
  }
}