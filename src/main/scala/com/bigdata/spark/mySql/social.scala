package com.bigdata.spark.mySql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object social {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("social").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("social").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("social").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val url= "jdbc:mysql://mysql.clrwwacyafx7.ap-south-1.rds.amazonaws.com:3306/pradeep"
    val username="muser"
    val password= "mpassword"
    val tab="social"
    val output=args(0)
    //val output="C:\\work\\impfiles\\data\\output\\avgsoocial"
    val drivername="com.mysql.cj.jdbc.Driver"

    val prop = new java.util.Properties()
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    prop.setProperty("driver",drivername)

    val df= spark.read.jdbc(url,tab,prop)
    df.createOrReplaceTempView("social")

    val res = spark.sql("select age,avg(friends) as average from social group by age order by age asc")
    res.show()
    //res.write.format("csv").option("header","true").save(output)
    res.write.jdbc(url,output,prop)

    spark.stop()
  }
}