name := "sparkproject"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"
val flinkVersion = "1.6.0"
val mysqlVersion = "6.0.6"
val calciteVersion = "1.16.0"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" % "flink-core" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-jdbc" % flinkVersion,
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
//"org.apache.flink" %% "flink-table" %  flinkVersion
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion
)
