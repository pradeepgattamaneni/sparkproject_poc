package com.bigdata.spark.mySql.IPL_Matches_Data

import com.bigdata.spark.sparkcore.Weather_analysis.minimum_temperature.data
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Best_sutable__to_BatFirst {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Best_sutable__to_BatFirst").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Best_sutable__to_BatFirst").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Best_sutable__to_BatFirst").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data ="C:\\work\\impfiles\\data\\matches.csv"
    val ipldf= sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(data)
    //ipldf.printSchema
    //val fil = iplrdd.map(x=>x.split(',')).filter(x=>x.length<19).map(x=>(x(7),x(11).toInt,x(12),x(14))).filter(x=>x._2==0)
    //val res = fil.map(x=>x._4).toDF("venue")

    val newdf = ipldf.toDF("id","season","city","date","team1","team2","toss_winner","toss_decision","result","dl_applied","winner","win_by_runs","win_by_wickets","player_of_match","venue","umpire1","umpire2","umpire3")
    newdf.createOrReplaceTempView("ipl")

    val result = spark.sql("select count(venue) wonfirst, venue from ipl where win_by_runs not like '0' group by venue order by wonfirst desc limit 10")
    result.show()

    spark.stop()
  }
}