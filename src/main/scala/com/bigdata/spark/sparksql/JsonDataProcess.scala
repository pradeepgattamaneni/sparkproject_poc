package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object JsonDataProcess {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("JsonDataProcess").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("JsonDataProcess").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("JsonDataProcess").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val data ="C:\\work\\impfiles\\data\\zips.json"
    //val df = spark.read.format("json").option("inferSchema","true").load(data)
    //df.createOrReplaceTempView("zips")
    //val res = spark.sql("select cast(_id as Int) id, city,loc[0] lat,loc[1] long,pop,state from zips where state like 'NY'")
    //val data = "C:\\work\\impfiles\\data\\world_bank.json"
    val data= args(0)
    val op= args(1)
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("worldbank")

    val res = spark.sql("select theme1.name themename,theme1.percent themepercent,tn.name themenamecodename,tn.code,sn.name snname,sn.code sncode from worldbank lateral view explode(theme_namecode) t as tn lateral view explode(sector_namecode) s as sn")
    res.show()
   // res.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(op)
    res.write.format("orc").saveAsTable(op)


    spark.stop()
  }
}