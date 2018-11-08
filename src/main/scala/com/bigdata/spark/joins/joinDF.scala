package com.bigdata.spark.joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object joinDF {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("joinDF").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("joinDF").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("joinDF").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    //data path
    val d1 ="C:\\work\\impfiles\\jyothi\\asl.csv"
    val d2 ="C:\\work\\impfiles\\jyothi\\nep.csv"
    //rdd creation
    val rdd1=sc.textFile(d1)
    val rdd2=sc.textFile(d2)
    //header
    val h1=rdd1.first()
    val h2=rdd2.first()

    //maping data
    val det1= rdd1.filter(x => x != h1).map(x => x.split(",")).map(x => (x(0), x(1), x(2))).toDF("name","age","city")
    val det2=rdd2.filter(x=>x!=h2).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","email","phone")
    /*//setting key
    val kr1=det1.keyBy(x=>x._1)
    val kr2=det2.keyBy(x=>x._1)
    //joining kr1 & Kr2

    val joinrdd=kr2.join(kr1)
    joinrdd.collect.foreach(println)

    val ziprdd=rdd1.zipWithIndex()
    ziprdd.collect.foreach(println)

    val res=joinrdd.map(x=>(x._1,x._2._1._2,x._2._1._3,x._2._2._2,x._2._2._3))
    res.collect.foreach(println)*/

    det1.createOrReplaceTempView("asl")
    det2.createOrReplaceTempView("nep")


    val res=spark.sql("select a.name,a.age,b.email,b.phone,a.city from asl a joins nep b where a.name=b.name")
    res.show()





    spark.stop()
  }
}