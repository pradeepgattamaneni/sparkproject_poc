package com.bigdata.spark.sparkcore.online_retail

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Best_Customer {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Best_Customer").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Best_Customer").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Best_Customer").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data= "C:\\work\\impfiles\\data\\online-retail-dataset.csv"
    val retaildf= sqlContext.read.format("csv").option("header", "true").load(data)
    val retailrdd = sc.textFile(data)
    val head= retailrdd.first()
    //val fil = retailrdd.filter(x=>x!=head).map(x=>x.split(","))
    val fil = retailrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(6),x(0))).distinct.map(x=>(x._1,1)).filter(x=>x._1 !="null").reduceByKey(_+_).sortBy(_._2,false)
   // fil.take(5).foreach(println)
    //retailrdd.createOrReplaceTempView("retail")
   // val distdf = retaildf.select("InvoiceNo".distinct,"CustomerId")
    val distdf = retaildf.select("InvoiceNo","CustomerId").distinct.groupBy($"CustomerId").count().where($"CustomerId"!=="null").sort($"count".desc)

    //distdf.createOrReplaceTempView("retail")
    //val res =spark.sql("select CustomerId,count(CustomerId) FROM retail where CustomerId not null group by CustomerId order by cnt desc")
    distdf.show(10)


    spark.stop()
  }
}