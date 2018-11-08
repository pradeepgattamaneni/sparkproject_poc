package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.nycrime.FBI_countCRIME.dataFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Avg_friends_for_age_group {
  case class Format(userId:String,name:String,age:Int,friends:Double)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Avg_friends_for_age_group").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Avg_friends_for_age_group").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Avg_friends_for_age_group").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val rdd = sc.textFile("C:\\work\\impfiles\\data\\social_friends.csv")
    val cdata =rdd.map(x=>x.split(",")).map(x=>Format(x(0),x(1),x(2).toInt,x(3).toDouble)).toDF()
    cdata.createOrReplaceTempView("social")
    val res = spark.sql("select age,avg(friends) as average from social group by age order by age asc")
    res.show()

    spark.stop()
  }
}