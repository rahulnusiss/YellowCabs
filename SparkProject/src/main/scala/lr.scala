import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.{LinearRegressionSummary, LinearRegression}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object gradient {


  def main(args: Array[String]) {

    println("Linear regression taxi")

    /**
      * Configurations for the Spark Application
      */
    val conf = new SparkConf().setAppName("Linear taxi Regression")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

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
        StructField("day_of_mon", IntegerType, true),
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

    // df.createOrReplaceTempView("taxi")
    //val df2 = df.filter("no_of_trips > 100000")
    //df2.show()   
    /**
      * Logic for Conversions to the RDD Data Type
      */
    val training_split = df.filter("ID <= 450")
    val testing_split = df.filter("ID > 450")

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val assembler = new VectorAssembler()
        .setInputCols(Array("rain", "snw", "temprature", "day_of_week", "mon_num"))
        .setOutputCol("features")

    val output_train = assembler.transform(training_split)
    val output_test = assembler.transform(testing_split)

    val trainingData = output_train.select("features", "no_of_trips")
    val testData = output_test.select("features", "no_of_trips")
    //val target = regModelData.columns.indexOf("label")
    println(trainingData)

    val model = new LinearRegression()
    model.setLabelCol("no_of_trips")
    val results = model.fit(trainingData)
    println("Printing results")
    println(results.coefficients)
    println(results.intercept)

    val trainingSummary = results.summary
    println("Printing Training Summary")
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"R2: ${trainingSummary.r2}")
    trainingSummary.residuals.show() 


    // Make predictions.
    val lr_test_summary = results.evaluate(testData)
    println("Printing Test Summary")
    println("Root Mean Squared Error (RMSE) on test data = " + lr_test_summary.rootMeanSquaredError)
    
    println("Mean absolute error on test data = " + lr_test_summary.meanAbsoluteError)


    val predValues:Array[Double] = lr_test_summary.predictions.select("prediction").rdd.map(_(0)).map(_.asInstanceOf[Double]).collect.toArray
    val actualValues:Array[Double] = testData.select("no_of_trips").rdd.map(_(0)).map(_.asInstanceOf[Double]).collect.toArray
    var sum = 0.0
    // Calculating MAPE Linear Gradient
    for( a <- 0 to 108){
        val temp = Math.abs(predValues(a) - actualValues(a))/actualValues(a)
        sum = sum + temp
           println( "Value of LinearRegression temp: " + temp )
        }

       println( "Value of Linear Regression MAPE: " + (sum/109) )

       // println("P values for linear regression: " + lr_test_summary.pValues.mkString("\n"))

      //sc.stop()
    
  }

}