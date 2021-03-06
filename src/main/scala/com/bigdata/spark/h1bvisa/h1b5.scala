package com.bigdata.spark.h1bvisa

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object h1b5 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("h1b5").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("h1b5").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("h1b4").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\work\\impfiles\\data\\h1b.csv")

    data.createOrReplaceTempView("h1b")
    val res = spark.sql("select EMPLOYER_NAME,COUNT(JOB_TITLE) AS count from h1b where JOB_TITLE like'DATA SCIENTIST' group by EMPLOYER_NAME order by count desc limit 5")
    res.show()
    spark.stop()
  }
}