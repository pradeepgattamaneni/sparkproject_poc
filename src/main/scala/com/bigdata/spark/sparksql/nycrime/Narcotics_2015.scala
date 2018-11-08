package com.bigdata.spark.sparksql.nycrime

import com.bigdata.spark.sparksql.nycrime.FBI_countCRIME.dataFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Narcotics_2015 {
  case class dataFormat(id:String, Primary_type:String,year:Int )
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Narcotics_2015").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Narcotics_2015").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Narcotics_2015").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val rdd = sc.textFile("C:\\work\\impfiles\\data\\Crimedata")
    val cdata =rdd.map(x=>x.split(",")).map(x=>dataFormat(x(0),x(5),x(17).toInt)).toDF()
    cdata.createOrReplaceTempView("nycrime")

    val res = spark.sql("select COUNT(Primary_type) as count from nycrime where Primary_type like 'NARCOTICS' and year like 2015")
    res.show()

    spark.stop()
  }
}