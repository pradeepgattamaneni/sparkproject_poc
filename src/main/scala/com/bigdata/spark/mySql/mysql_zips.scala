package com.bigdata.spark.mySql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object mysql_zips {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("mysql_zips").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("mysql_zips").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("mysql_zips").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val url= "jdbc:mysql://localhost:3305/pradeep?useSSL=false"
    val username="root"
    val password= "mpassword"
    //val tab="social"
    //val output=args(0)
    //val output="C:\\work\\impfiles\\data\\output\\avgsoocial"
    val drivername="com.mysql.cj.jdbc.Driver"

    val prop = new java.util.Properties()
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    prop.setProperty("driver",drivername)

    val data="C:\\work\\impfiles\\data\\zips.json"
    val df= spark.read.format("json").option("inferSchema","true").load(data)
    //val df= spark.read.jdbc(url,tab,prop)
    df.createOrReplaceTempView("zips")

    val res = spark.sql("select cast(_id as Int) id, city,loc[0] lat,loc[1] long,pop,state from zips")
    res.show()
    //res.write.format("csv").option("header","true").save(output)
    res.write.jdbc(url,"zips",prop)

    spark.stop()
  }
}