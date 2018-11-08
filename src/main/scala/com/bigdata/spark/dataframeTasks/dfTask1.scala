package com.bigdata.spark.dataframeTasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object dfTask1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("dfTask1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dfTask1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("dfTask1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    //task1
    val data1="C:\\work\\impfiles\\data\\employees.csv"
    val rdd1=sc.textFile(data1)
    val erdd1=rdd1.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17)))
    val df1=erdd1.toDF("EmployeeID","LastName","FirstName","Title","TitleOfCourtesy","BirthDate","HireDate","Address","City","Region","PostalCode","Country","HomePhone","Extension","Photo","Notes","ReportsTo","PhotoPath")
    val timestmp=df1.withColumn("BirthDate",$"BirthDate".cast("timestamp")).withColumn("HireDate",$"HireDate".cast("timestamp"))
    timestmp.printSchema()

    //task2
    val data2="C:\\work\\impfiles\\data\\suppliers.csv"
    //val rdd2=sc.textFile(data2)
    val df2=spark.read.format("csv").option("header","true").option("inferschema","true").load(data2)
    df2.createOrReplaceTempView("suppliers")
    //val res2=df2.select("SupplierID","CompanyName").filter($"CompanyName"==="Exotic Liquids"||"Grandma Kelly''s Homestead"||"Tokyo Traders")
    val data3="C:\\work\\impfiles\\data\\products.csv"
    val df3=spark.read.format("csv").option("header","true").option("inferschema","true").load(data3)
    df3.createOrReplaceTempView("products")
   // val res2=spark.sql("select b.ProductName,b.SupplierID from products b,suppliers a where ")




    spark.stop()
  }
}