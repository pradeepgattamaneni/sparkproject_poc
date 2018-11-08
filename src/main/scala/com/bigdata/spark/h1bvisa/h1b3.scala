package com.bigdata.spark.h1bvisa

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object h1b3 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("h1b3").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("h1b3").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("h1b3").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data= sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\work\\impfiles\\data\\h1b.csv")

    data.createOrReplaceTempView("h1b")
    val res = spark.sql("select worksite,Tjob,year from(Select rank() over(partition by year order by Tjob desc) as rank_1,Tjob,worksite,year from(Select count(job_title)as Tjob,worksite,year from h1b where job_title like 'DATA ENGINEER' group by worksite,year order by Tjob) a)b where rank_1=1 and year is not null order by year asc")

    res.show()

    //select worksite,Tjob,year from(Select rank() over(partition by year order by Tjob desc) as rank_1,Tjob,worksite,year from(Select count(job_title)as Tjob,worksite,year from h1b_final where job_title="DATA ENGINEER" group by worksite,year order by Tjob) a)b where rank_1=1 and year is not null

    spark.stop()
  }
}