

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts/0.4.0
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.sql.types._

// https://github.com/sryza/spark-timeseries/blob/master/src/main/scala/com/cloudera/sparkts/models/ARIMA.scala
/**
 * An example showcasing the use of ARIMA in a non-distributed context.
 */
object TaxiARIMA {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.

    println("ARIMA taxi trips")

    /**
      * Configurations for the Spark Application
      */
    val conf = new SparkConf().setAppName("Regression")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Create schema for input csv.
    val customSchema = StructType(Array(
        StructField("date", StringType, true),
        StructField("date_id", StringType, true),
        StructField("amount", DoubleType, true),
        StructField("passenger_count", DoubleType, true),
        StructField("distance_travelled", DoubleType, true),
        StructField("no_of_trips", DoubleType, true),
        StructField("date_id2", StringType, true),
        StructField("transaction_date", StringType, true),
        StructField("year", IntegerType, true),
        StructField("mon_year", StringType, true),
        StructField("year_mon_num", StringType, true),
        StructField("qtr_num", IntegerType, true),
        StructField("qtr_year", StringType, true),
        StructField("mon_num", IntegerType, true),
        StructField("mon_long_name", StringType, true),
        StructField("mon_short_name", StringType, true),
        StructField("wk_num", DoubleType, true),
        StructField("day_of_year", DoubleType, true),
        StructField("day_of_mon", DoubleType, true),
        StructField("day_of_week", IntegerType, true),
        StructField("day_long_name", StringType, true),
        StructField("day_short_name", StringType, true),
        StructField("temprature", DoubleType, true),
        StructField("day", StringType, true),
        StructField("rain", IntegerType, true),
        StructField("snw", IntegerType, true),
        StructField("ID", IntegerType, true)))

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(customSchema)
      .load("Taxi_Data.csv")

    // Split the data in training and test 80:20
    val df_train = df.filter("ID <= 450")
    val df_test = df.filter("ID > 450")
    df_test.show()

    // Select vector no of trips only
    val lines_vector = df_train.select("no_of_trips").rdd.map(_(0))
    val lines_test = df_test.select("no_of_trips").rdd.map(_(0))

    // ARIMA model accepts vector of double
    val nar:Array[Double] = lines_vector.map(_.asInstanceOf[Double]).collect.toArray
    val nar_test:Array[Double] = lines_test.map(_.asInstanceOf[Double]).collect.toArray
    
    // Fit ARIMA model on no of trips
    val ts = Vectors.dense(nar)
    val arimaModel = ARIMA.autoFit(ts)
    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 109)
    println("forecast of next 20 observations: " + forecast.toArray.mkString("\n"))

    // Calculating MAPE
    var sum = 0.0
    val future_prediction: Array[Double] = forecast.toArray
    for( a <- 0 to 108){
    	val temp = Math.abs(future_prediction(a) - nar_test(a))/nar_test(a)
    	sum = sum + temp
         println( "Value of temp: " + temp )
      }

     println( "Value of MAPE: " + (sum/109) )

     sc.stop()
  }
}