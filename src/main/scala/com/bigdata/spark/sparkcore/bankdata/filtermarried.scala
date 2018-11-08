package com.bigdata.spark.sparkcore.bankdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object filtermarried {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("filtermarried").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("filtermarried").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("filtermarried").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data= "C:\\work\\impfiles\\data\\bank-full.csv"
    val retailrdd = sc.textFile(data)
    val head= retailrdd.first()
    val res = retailrdd.filter(x=>x!=head).map(x=>x.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")).map(x=>(x(0),x(1),x(2),x(3)))
    res.take(1).foreach(println)

    spark.stop()
  }
}