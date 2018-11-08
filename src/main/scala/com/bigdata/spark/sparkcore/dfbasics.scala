package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object dfbasics {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("dfbasics").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dfbasics").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("dfbasics").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\impfiles\\data\\diamonds.csv"

    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

    //df.show()

    df.printSchema()
    val df1 = df.withColumnRenamed("_c0","id")

    //val df2 = df1.withColumn("Note", lit("note"))
    //df2.show()

    spark.stop()
  }
}