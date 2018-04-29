

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.sql.types._
/**
 * An example showcasing the use of ARIMA in a non-distributed context.
 */
object SingleSeriesARIMA {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.

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
        StructField("RAIN", IntegerType, true),
        StructField("SNW", IntegerType, true),
        StructField("DATE_ID_2", StringType, true),
        StructField("DAYNUM", DoubleType, true)))

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(customSchema)
      .load("DATA_MODEL_R_S.csv")


    val df_train = df.filter("DAYNUM <= 500")
    val df_test = df.filter("DAYNUM > 500")
    df_test.show()

    val lines_vector = df_train.select("TRIPS").rdd.map(_(0))//.collect//.toList
    val lines_test = df_test.select("TRIPS").rdd.map(_(0))

    val nar:Array[Double] = lines_vector.map(_.asInstanceOf[Double]).collect.toArray
    val nar_test:Array[Double] = lines_test.map(_.asInstanceOf[Double]).collect.toArray


    // val testing_vector = lines_vector.drop(500)
    // val nar_test:Array[Double] = testing_vector.map(_.asInstanceOf[Double]).collect.toArray
    // nar_test.foreach(println)

    // df.select("id").map(_.getString(0)).collect.toList

    val ts = Vectors.dense(nar)
    val arimaModel = ARIMA.autoFit(ts)
    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 59)
    println("forecast of next 20 observations: " + forecast.toArray.mkString("\n"))

    var sum = 0.0
    val future_prediction: Array[Double] = forecast.toArray
    for( a <- 0 to 58){
    	val temp = Math.abs(future_prediction(a) - nar_test(a))/nar_test(a)
    	sum = sum + temp
         println( "Value of temp: " + temp )
      }

     println( "Value of MAPE: " + (sum/59) )
  }
}