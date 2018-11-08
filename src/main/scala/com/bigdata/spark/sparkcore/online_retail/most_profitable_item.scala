package com.bigdata.spark.sparkcore.online_retail

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object most_profitable_item {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("most_profitable_item").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("most_profitable_item").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("most_profitable_item").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data= "C:\\work\\impfiles\\data\\online-retail-dataset.csv"
    val retailrdd = sc.textFile(data)
    val head= retailrdd.first()
    val fil = retailrdd.filter(x=>x!=head).map(x=>x.split(",")).filter(x=>x(7)== "United Kingdom")
    val res = fil.map(x=>((x(1),x(2),x(7)),x(3).toDouble*x(5).toDouble)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    res.take(1).foreach(println)

    spark.stop()
  }
}