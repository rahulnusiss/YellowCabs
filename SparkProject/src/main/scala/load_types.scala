import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object loadSchema {
  def main(args: Array[String]): Unit = {
    

    println("Linear regression taxi")

    /**
      * Configurations for the Spark Application
      */
    val conf = new SparkConf().setAppName("Linear taxi Regression")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val customSchema = StructType(Array(
        StructField("DATE_ID", StringType, true),
        StructField("AMOUNT", DoubleType, true),
        StructField("PASSENGER_COUNT", DoubleType, true),
        StructField("DISTANCE", DoubleType, true),
        StructField("TRIPS", DoubleType, true),
        StructField("DATE_ID_1", StringType, true),
        StructField("TRANSACTION_DATE", StringType, true),
        StructField("YEAR", IntegerType, true),
        StructField("MON_YEAR", StringType, true),
        StructField("YEAR_MON_NUM", StringType, true),
        StructField("QTR_NUM", IntegerType, true),
        StructField("QTR_YEAR", StringType, true),
        StructField("MON_NUM", DoubleType, true),
        StructField("MON_LONG_NAME", StringType, true),
        StructField("MON_SHORT_NAME", StringType, true),
        StructField("WK_NUM", DoubleType, true),
        StructField("DAY_OF_YEAR", DoubleType, true),
        StructField("DAY_OF_MON", DoubleType, true),
        StructField("DAY_OF_WEEK", IntegerType, true),
        StructField("DAY_LONG_NAME", StringType, true),
        StructField("DAY_SHORT_NAME", StringType, true),
        StructField("AVG_TEMP", DoubleType, true),
        StructField("DAY", StringType, true),
        StructField("RAIN", StringType, true),
        StructField("SNW", StringType, true),
        StructField("DATE_ID_2", StringType, true),
        StructField("DAYNUM", DoubleType, true)))


     val pagecount = sqlContext.read.format("com.databricks.spark.csv")
             .option("inferSchema", "true")
             .option("header", "true")
             .schema(customSchema)
             .load("DATA_MODEL.csv")

    val trainingData = pagecount.limit(10)

    val lines_vector = trainingData.select("TRIPS").rdd.map(_(0)).collect.toList
    lines_vector.foreach(println)




    //DATE_ID,AMOUNT,PASSENGER_COUNT,DISTANCE,TRIPS,DATE_ID_1,TRANSACTION_DATE,YEAR,MON_YEAR,YEAR_MON_NUM,QTR_NUM,QTR_YEAR,MON_NUM,MON_LONG_NAME,MON_SHORT_NAME,WK_NUM,DAY_OF_YEAR,DAY_OF_MON,DAY_OF_WEEK,DAY_LONG_NAME,DAY_SHORT_NAME,AVG_TEMP,DAY,RAIN,SNW,DATE_ID_2,DAYNUM

    // .format("com.databricks.spark.csv")
    // .option("header", "true")
    //   .option("inferSchema", "true")
    //   .load("DATA_MODEL.csv")

      pagecount.show()
    
  }
}