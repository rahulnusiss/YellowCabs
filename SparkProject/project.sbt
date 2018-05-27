name := "Spark2Demo"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "mvnrepository" at "https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion  
)

libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"