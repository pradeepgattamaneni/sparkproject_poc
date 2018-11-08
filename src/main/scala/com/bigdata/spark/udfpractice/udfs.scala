package com.bigdata.spark.udfpractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object udfs {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("udfs").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("udfs").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("udfs").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data="C:\\work\\impfiles\\data\\bank-full.csv"
    val df = spark.read.format("csv").option("delimiter",";").option("inferSchema","true").option("header","true").load(data)
    //df.show()
    //df.printSchema()
    df.createOrReplaceTempView("tab")

    def ageoffer (age:Int)= age match{
      case x if x<=60 =>"no offer"
      case x if x>60 && x<=80 => "30% discount"
      case x if x>80 && x<=100 => "50% discount"
      case _ =>"no offer"
    }
    val afunc = udf(ageoffer _)
    val res=df.select("age","job","balance").where($"age".gt(70)).withColumn("newoff",afunc($"age"))
    /*spark.udf.register("abcd",afunc)
    // where age=(select max(age) from tab)

    val res=spark.sql("select age,job,balance,abcd(age) as offer from tab where age>58 ")*/
    res.show()

    spark.stop()
  }
}