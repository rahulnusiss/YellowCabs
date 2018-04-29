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

    df.createOrReplaceTempView("taxi")
    val df2 = df.filter("TRIPS > 100000")
    df2.show()   
    /**
      * Logic for Conversions to the RDD Data Type
      */
    val assembler = new VectorAssembler()
      .setInputCols(Array("RAIN", "SNW", "AVG_TEMP"))
      .setOutputCol("features")

    val output = assembler.transform(df2)

    val regModelData = output.select("features", "TRIPS")
    //val target = regModelData.columns.indexOf("label")
    regModelData.show()

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = regModelData.randomSplit(Array(0.6, 0.4))

    val model = new LinearRegression()
    model.setLabelCol("TRIPS")
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

    // // Select example rows to display.
    // predictions.select("prediction", "TRIPS", "features").show(5)

    // // Select (prediction, true label) and compute test error
    // val evaluator = new RegressionEvaluator()
    //   .setLabelCol("TRIPS")
    //   .setPredictionCol("prediction")
    //   .setMetricName("rmse")
    // val rmse = evaluator.evaluate(predictions)
    println("Printing Test Summary")
    println("Root Mean Squared Error (RMSE) on test data = " + lr_test_summary.rootMeanSquaredError)

    // val evaluator2 = new RegressionEvaluator()
    //   .setLabelCol("TRIPS")
    //   .setPredictionCol("prediction")
    //   .setMetricName("r2")

    // val r2 = evaluator.evaluate(predictions)
    println("R2(R2) on test data = " + lr_test_summary.r2)

    //sc.stop()
    
  }

}