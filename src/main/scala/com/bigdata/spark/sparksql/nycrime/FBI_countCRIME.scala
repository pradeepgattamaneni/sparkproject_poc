package com.bigdata.spark.sparksql.nycrime

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object FBI_countCRIME {
  //case class dataFormat(id:String, CaseNumber:String, Date:String, Block:String, IUCR:String, Primary_type:String, Description:String, Location_description:String, Arrest:String, Domestic:String, Beat:String, District:String, Ward:String, community:String, Fbicode:String, X_Co_ordinate:String, Y_Co_ordinate:String, Year:String, Updated_on:String, lattitude:String, longititude:String, loctation:String)
  case class dataFormat(id:String,Fbicode:String)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("FBI_countCRIME").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("FBI_countCRIME").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("FBI_countCRIME").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val rdd = sc.textFile("C:\\work\\impfiles\\data\\Crimedata")
    //val cdata=rdd.map(x=>x.split(",")).map(x=>dataFormat(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21))).toDF()
    val cdata =rdd.map(x=>x.split(",")).map(x=>dataFormat(x(0),x(14))).toDF()
    cdata.createOrReplaceTempView("nycrime")

    val res = spark.sql("select Fbicode,COUNT(Fbicode) as count from nycrime group by Fbicode order by count desc")
    res.show()


    spark.stop()
  }
}