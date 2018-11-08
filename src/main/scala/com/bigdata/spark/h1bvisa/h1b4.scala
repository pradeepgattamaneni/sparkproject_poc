package com.bigdata.spark.h1bvisa

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object h1b4 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("h1b4").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("h1b4").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("h1b4").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\work\\impfiles\\data\\h1b.csv")

    data.createOrReplaceTempView("h1b")
    val res = spark.sql("select worksite,Tcity,year from(Select rank() over(partition by year order by Tcity desc)as rank_1,worksite,Tcity,year from(Select count(worksite)as Tcity,worksite,year from h1b where case_status=\"CERTIFIED\" or case_status=\"CERTIFIED-WITHDRAWN\" group by worksite,year) a)b where rank_1<6 and year is not null")
    res.show()

    spark.stop()
  }
}