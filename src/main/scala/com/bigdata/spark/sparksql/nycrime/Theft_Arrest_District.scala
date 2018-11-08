package com.bigdata.spark.sparksql.nycrime

import com.bigdata.spark.sparksql.nycrime.Narcotics_2015.dataFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Theft_Arrest_District {
  case class dataFormat(arrest:String, Primary_type:String,district:String )
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Theft_Arrest_District").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Theft_Arrest_District").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Theft_Arrest_District").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val rdd = sc.textFile("C:\\work\\impfiles\\data\\Crimedata")
    val cdata =rdd.map(x=>x.split(",")).map(x=>dataFormat(x(8),x(5),x(11))).toDF()
    cdata.createOrReplaceTempView("nycrime")

    val res = spark.sql("select district,COUNT(district) as count from nycrime where Primary_type like '%THEFT%' and arrest like 'true' group by district order by count")
    res.show()

    spark.stop()
  }
}