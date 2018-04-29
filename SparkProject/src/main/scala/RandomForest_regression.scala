import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._

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

    val data = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("DATA_MODEL.csv")

	// Automatically identify categorical features, and index them.
	// Set maxCategories so features with > 4 distinct values are treated as continuous.
	val assembler = new VectorAssembler()
      .setInputCols(Array("DAYNUM","WK_NUM", "DAY_OF_YEAR", "DAY_OF_MON", "DAY_OF_WEEK"))
      .setOutputCol("features")

    val output = assembler.transform(data)

    val regModelData = output.select("features", "TRIPS")
    //val target = regModelData.columns.indexOf("label")
    println(regModelData)

	  // Split the data into training and test sets (30% held out for testing).
	val Array(trainingData, testData) = regModelData.randomSplit(Array(0.7, 0.3))	

	// Train a RandomForest model.
	val rf = new RandomForestRegressor()
	  .setLabelCol("TRIPS")
	  .setFeaturesCol("features")

	// Train model. This also runs the indexer.
	val model = rf.fit(trainingData)
	
	// Make predictions.
	val predictions = model.transform(testData)

	// Select example rows to display.
	predictions.select("prediction", "TRIPS", "features").show(5)

	println("Printing test summary")
	// Select (prediction, true label) and compute test error.
	val evaluator = new RegressionEvaluator()
	  .setLabelCol("TRIPS")
	  .setPredictionCol("prediction")
	  .setMetricName("rmse")
	val rmse = evaluator.evaluate(predictions)
	println("Root Mean Squared Error (RMSE) on test data = " + rmse)

	// Select (prediction, true label) and compute test error.
	val evaluator2 = new RegressionEvaluator()
	  .setLabelCol("TRIPS")
	  .setPredictionCol("prediction")
	  .setMetricName("r2")
	val r2 = evaluator2.evaluate(predictions)
	println("R2 value on test data = " + r2)

   //sc.stop()
    
  }

}