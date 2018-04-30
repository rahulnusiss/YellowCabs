import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

object random_gradient {


  def main(args: Array[String]) {

    println("Linear regression taxi")

    /**
      * Configurations for the Spark Application
      */
    val conf = new SparkConf().setAppName("Linear taxi Regression")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Build schema
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

    val data = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(customSchema)
      .load("Taxi_Data.csv")


    // Split 80:20
    val training_split = data.filter("ID <= 450")
    val testing_split = data.filter("ID > 450")

	// Assemble input columns
	val assembler = new VectorAssembler()
      .setInputCols(Array("rain", "snw", "temprature", "day_of_week", "mon_num", "day_of_mon", "day_of_year"))
      .setOutputCol("features")

    val output_train = assembler.transform(training_split)
    val output_test = assembler.transform(testing_split)

    val trainingData = output_train.select("features", "no_of_trips")
    val testData = output_test.select("features", "no_of_trips")
    //val target = regModelData.columns.indexOf("label")
    println(trainingData)	

	// Train a RandomForest model.
	val rf = new RandomForestRegressor()
	  .setLabelCol("no_of_trips")
	  .setFeaturesCol("features")

	// Train model.
	val model = rf.fit(trainingData)
	
	// Make predictions.
	val predictions = model.transform(testData)

	// Select example rows to display.
	predictions.select("prediction", "no_of_trips", "features").show()

	println("Printing test summary")
	// Select (prediction, true label) and compute test error.
	val evaluator = new RegressionEvaluator()
	  .setLabelCol("no_of_trips")
	  .setPredictionCol("prediction")
	  .setMetricName("rmse")
	val rmse = evaluator.evaluate(predictions)
	println("Root Mean Squared Error (RMSE) on test data = " + rmse)

	// Select (prediction, true label) and compute test error.
	val evaluator2 = new RegressionEvaluator()
	  .setLabelCol("no_of_trips")
	  .setPredictionCol("prediction")
	  .setMetricName("r2")
	val r2 = evaluator2.evaluate(predictions)
	// R2 not showing correct result
	println("R2 value on test data = " + r2)


	val predValues:Array[Double] = predictions.select("prediction").rdd.map(_(0)).map(_.asInstanceOf[Double]).collect.toArray
	val actualValues:Array[Double] = predictions.select("no_of_trips").rdd.map(_(0)).map(_.asInstanceOf[Double]).collect.toArray
	var sum = 0.0
	// Calculating MAPE Random Forest
	for( a <- 0 to 108){
    	val temp = Math.abs(predValues(a) - actualValues(a))/actualValues(a)
    	sum = sum + temp
         println( "Value of RandomForest temp: " + temp )
      }

     println( "Value of RandomForest MAPE: " + (sum/109) )

   sc.stop()
    
  }

}