package com.bigdata.spark.h1bvisa

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object h1b1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("h1b1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("h1b1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("h1b1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    //val data = "file:///C:\\work\\impfiles\\data\\h1b.csv"
    val data= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\work\\impfiles\\data\\h1b.csv")

    //val h1bdata=h1brdd.map(x=>x.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")).map(x=>(x(7),x(4))).toDF("year","job")
    data.createOrReplaceTempView("h1b")
    val res = spark.sql("select YEAR,Count(JOB_TITLE) from h1b where JOB_TITLE like '%DATA ENGINEER%' group by YEAR order by YEAR desc")
    res.show()

   // res.collect().foreach(println)


    spark.stop()
  }
}