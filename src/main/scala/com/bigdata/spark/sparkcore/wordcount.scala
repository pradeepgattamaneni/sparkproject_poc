package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object wordcount {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("wordcount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val num = 1 to 20 toArray
   // val numrdd = sc.parallelize(num)
   // val res=numrdd.map(x=>x*x).filter(x=>x<100)
    //res.collect().foreach(println)

    val data = "file:///C:\\work\\impfiles\\data\\sample.txt"
    val samrdd = sc.textFile(data)
    val result = samrdd.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+"," ")).flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).sortBy(x=>x._2,false)
    result.collect().foreach(println)
      spark.stop()
  }
}