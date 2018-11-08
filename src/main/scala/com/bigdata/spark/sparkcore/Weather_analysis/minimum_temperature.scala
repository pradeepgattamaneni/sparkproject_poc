package com.bigdata.spark.sparkcore.Weather_analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object minimum_temperature {
  case class data(temp:Double,station:String)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("minimum_temperature").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("minimum_temperature").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("minimum_temperature").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val weatherrdd = sc.textFile("C:\\work\\impfiles\\data\\Temperature.csv")
    val fil = weatherrdd.map(x=>x.split(',')).map(x=>(x(0),x(2),x(3).toDouble)).filter(x=>x._2=="TMIN")
    //val fil = weatherrdd.map(x=>x.split(',')).filter(x=>{if(x(2).matches("TMIN"))true else false})
    val res = fil.map(x=>data(x._3,x._1)).toDF()
    res.createOrReplaceTempView("weather")

    val result = spark.sql("select station,(temp*0.1*(9.0/5.0)+32.0) as temperature from weather order by temperature asc limit 10")

   result.show()

    spark.stop()
  }
}