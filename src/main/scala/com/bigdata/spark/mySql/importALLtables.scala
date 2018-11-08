package com.bigdata.spark.mySql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object importALLtables {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importALLtables").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importALLtables").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("importALLtables").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val tables = Array("EMP","DEPT","BONUS","SALGRADE")
    tables.foreach { x =>
      val url = "jdbc:oracle:thin://@orcl.clrwwacyafx7.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val username = "ousername"
      val password = "opassword"
      // val tab=""

      val drivername = "oracle.jdbc.OracleDriver"

      val prop = new java.util.Properties()
      prop.setProperty("user", username)
      prop.setProperty("password", password)
      prop.setProperty("driver", drivername)

      val db1 = spark.read.jdbc(url,x,prop)

      db1.show()
    }



    spark.stop()
  }
}