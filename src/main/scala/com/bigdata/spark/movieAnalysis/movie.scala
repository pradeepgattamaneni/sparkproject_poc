package com.bigdata.spark.movieAnalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object movie {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("movie").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("movie").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("movie").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val rdd = sc.textFile("C:\\work\\impfiles\\data\\popular_movies.txt")
    val mvdata=rdd.map(x=>x.split("\t")).map(x=>(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt)).toDF("userid","movieid","rating","timestamp")
    mvdata.createOrReplaceTempView("mvdata")
    val res = spark.sql("select movieid,count(movieid) as occurrence from mvdata group by movieid order by occurrence desc limit 5")
    res.show()



    spark.stop()
  }
}