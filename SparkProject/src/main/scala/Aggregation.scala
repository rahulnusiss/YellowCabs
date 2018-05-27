// case class for pattern matching
case class Taxi_loc_class(realdate_loc: String, date_1: String, amount_loc: Double, passenger_loc: Int, distance_loc: Double, tripcount_loc: Int,start_loc: String, end_loc: String)
case class Taxi_class(realdate: String, date: String, amount: Double, passenger: Int, distance: Double, tripcount: Int)
case class Taxi_class1(realdate: String, date: String, amount: Double, passenger: Int, distance: Double, tripcount: Int, hour: String)

// Import libraries
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j._
import java.io.IOException
import java.io.FileNotFoundException

object AggregationCode_FinalVersion {
   def main(args: Array[String]) {

val ProgramStartTime= Calendar.getInstance.getTime

//val csv = sc.textFile("/E:/Data/yellow_tripdata_2017-02.csv");

// Input path and filename
var inputPath = ""
val year = List("2016","2017")
val month = List("01","02","03","04","05","06","07","08","09","10","11","12")
var taxiyear = ""
var taximonth = ""
var count : Long = 0L

for( taxiyear <- year ) {
  for( taximonth <- month ) {
    try {
      if(!( taxiyear=="2016" && (taximonth== "01" || taximonth== "02" || taximonth== "03" || taximonth== "04" || taximonth== "05" || taximonth== "06") )) {
        val conf =  new SparkConf().setAppName("Taxi").setMaster("local")//.set("spark.storage.memoryFraction", "1")
        val sc = new SparkContext(conf)  // Spark context object
        val mySpark = SparkSession.builder().appName("Spark SQL").config("spark.some.config.option", "some-value").getOrCreate() // taking sparksession
        import mySpark.implicits._   // spark session with implicit conversion, like conversion from RDD to Dataframe
        println(s"Data extraction started for Lift: $taxiyear ; date: $taximonth")
        inputPath = "/E:/Data/yellow_tripdata_"+ "" + taxiyear + "-" + taximonth + ".csv"
        println(s"Filename is : $inputPath ")

        //"/C:/Data/yellow_tripdata_2016-07.csv"
        val csv = sc.textFile(inputPath) // reading the file as RDD
        val headerAndRows = csv.map(line => line.split(",").map(_.trim)) 
        
        val header = headerAndRows.first // get header

        // filter out header (ex: just check if the first val matches the first header name)
        val data = headerAndRows.filter(_(0) != header(0))
        
        // Filter the null row
        val rdd = data.mapPartitionsWithIndex( (i, iterator) => if (i == 0 && iterator.hasNext) {
        iterator.next
        iterator
        } else iterator)
        // println(rdd1.count())
        
        //mapping the individual columns
        val rdd1 = rdd.map(row => (row(0), row(1).takeWhile(_ != ' '), row(2), row(3), row(4), row(5), row(6), row(7), row(8),row(9), row(10), row(11), row(12), row(13), row(14), row(15), row(16),  1, row(1).takeWhile(_ != ' ').replaceAll("-","")))

        // Filtering rdd  '0' and null values in 'Trip Distance', 'Trip amount' and 'Distance travelled'
        val rdd1_1 = rdd1.filter(x => (x._5 != ".00")).filter(x => (x._4 != "0")).filter(x => (x._17 != "0"))

        // Handling cancelled trips (HAndled trip count, PAssenger travelled and distance travelled)
        val rdd2 = rdd1_1.map(x => (x._1,x._2,x._3,if(x._17.toDouble < 0.00) -x._4.toInt else x._4.toInt ,if(x._17.toDouble < 0.00) -x._5.toDouble else x._5.toDouble, x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16,x._17, if(x._17.toDouble < 0.00) -x._18.toInt else x._18.toInt ,x._19))
         
         //---------------------------------------------------------------------------------------------------------//
         // Table      :    Day wise data aggregation for Amount, PassengerCount, Distance Travelled and TripCount  //
         //---------------------------------------------------------------------------------------------------------//
        
        // Group 'Date' and then aggregate variables (amount,passenger,distance, tripcount) and then sort the records with respect to 'Date'
        // amount rdd
        val rdd4 = rdd2.map(x => (x._2, x._17.toFloat)).map { case (date,  amnt) => (date, (amnt)) }.reduceByKey(_ + _).map { case (date,  amnt) => ((date), amnt) }.sortByKey(true)
        // passenger
        val rdd5 = rdd2.map(x => (x._2, x._4.toInt)).map { case (date, passenger) => (date, (passenger)) }.reduceByKey(_ + _).map { case (date, passenger) => ((date), passenger) }.sortByKey(true)
        // distance
        val rdd6 = rdd2.map(x => (x._2, x._5.toFloat)).map { case (date, distance) => (date, (distance)) }.reduceByKey(_ + _).map { case (date,  distance) => ((date), distance) }.sortByKey(true)
        // tripcount
        val rdd7 = rdd2.map(x => (x._2, x._18.toInt)).map { case (date, tripcount) => (date, (tripcount)) }.reduceByKey(_ + _).map { case (date,  tripcount) => ((date), tripcount) }.sortByKey(true)
        
        // Converting rdd to DF
        val amount_df = rdd4.toDF("date","amount")

        val realdate = amount_df.select("date").collect().map(_(0)).toArray.map(_.toString)
        val amount = amount_df.select("amount").collect().map(_(0)).toArray.map(_.toString)
        val passenger = rdd5.toDF("date","passenger").select("passenger").collect().map(_(0)).toArray.map(_.toString)
        val distance = rdd6.toDF("date","distance").select("distance").collect().map(_(0)).toArray.map(_.toString)
        val tripcount = rdd7.toDF("date","tripcount").select("tripcount").collect().map(_(0)).toArray.map(_.toString)

        val inputFormat = new SimpleDateFormat("yyyy-MM-dd")
        val outputFormat = new SimpleDateFormat("ddMMyyyy")
        val date =  amount_df.select("date").collect().map(_(0)).toArray.map(_.toString.trim).map(num => outputFormat.format(inputFormat.parse(num)))

        val taxi_array = Array(realdate, date, amount,passenger,  distance, tripcount).transpose
        val taxi_rdd = sc.parallelize(taxi_array).map(col => Taxi_class(col(0).toString,col(1).toString, col(2).toDouble, col(3).toInt, col(4).toDouble ,col(5).toInt ))
        val taxi_df = taxi_rdd.toDF("realdate","date","amount","passenger","distance","tripcount")
        
        //saving the df as file in hdfs ('Day' wise data aggragation)
        taxi_df.coalesce(1).write.format("csv").mode("append").save("hdfs://quickstart.cloudera/datada/2017.csv") 
        
         //----------------------------------------------------------------------------------------------------------------------------------------------//
         // Table      : (Start location to End location for each day) - data aggregation for Amount, PassengerCount, Distance Travelled and TripCount   //
         //----------------------------------------------------------------------------------------------------------------------------------------------//
        // Group 'A to B' location for each date and then aggregate variables (amount,passenger,distance, tripcount) 
        // amount
        val rdd9 = rdd2.map(x => (x._2, x._8,x._9, x._17.toFloat)).map{ case (date, sloc, eloc, amnt) => ((date, sloc, eloc), (amnt))}.reduceByKey(_ + _)

        //Passenger
        val rdd10 = rdd2.map(x => (x._2, x._8,x._9, x._4.toInt)).map { case (date, sloc, eloc, passenger) => ((date, sloc, eloc), (passenger)) }.reduceByKey(_ + _)

        //distance
        val rdd11 = rdd2.map(x => (x._2, x._8,x._9, x._5.toFloat)).map { case (date, sloc, eloc, distance) => ((date, sloc, eloc), (distance)) }.reduceByKey(_ + _)

        //tripcount
        val rdd12 = rdd2.map(x => (x._2, x._8,x._9, x._18)).map { case (date, sloc, eloc, tripcount) => ((date, sloc, eloc), (tripcount)) }.reduceByKey(_ + _)

        val rdd13 = rdd9.map(x => (x._1))
        
        // converting RDD to DF
        val datedf = rdd13.toDF("date","startloc","endloc")
        val amount_loc = rdd9.toDF("date_loc","amount").select("amount").collect().map(_(0)).toArray.map(_.toString)
        val passenger_loc = rdd10.toDF("date_loc","passenger").select("passenger").collect().map(_(0)).toArray.map(_.toString)
        val distance_loc = rdd11.toDF("date_loc","distance").select("distance").collect().map(_(0)).toArray.map(_.toString)
        val tripcount_loc = rdd12.toDF("date_loc","tripcount").select("tripcount").collect().map(_(0)).toArray.map(_.toString)
        val date_loc = datedf.select("date").collect().map(_(0)).toArray.map(_.toString)
        val start_loc = datedf.select("startloc").collect().map(_(0)).toArray.map(_.toString)
        val end_loc = datedf.select("endloc").collect().map(_(0)).toArray.map(_.toString)

        val date_1 =  datedf.select("date").collect().map(_(0)).toArray.map(_.toString.trim).map(num => outputFormat.format(inputFormat.parse(num)))

        val taxi_loc_array = Array(date_loc, date_1, amount_loc,passenger_loc,  distance_loc, tripcount_loc,start_loc,end_loc).transpose

        val taxi_loc_rdd = sc.parallelize(taxi_loc_array).map(col => Taxi_loc_class(col(0).toString,col(1).toString, col(2).toDouble, col(3).toInt, col(4).toDouble ,col(5).toInt,col(6).toString,col(7).toString ))
        val taxi_loc_df = taxi_loc_rdd.toDF("realdate","date","amount","passenger","distance","tripcount","startlocation","endlocation")

        // Saving the file to hdfs (Day wise, start loc -> end loc aggretion)
        taxi_loc_df.coalesce(1).write.format("csv").mode("append").save("hdfs://quickstart.cloudera/datada/2017_table1.csv")

         //--------------------------------------------------------//
         //               Aggregating passenger count and amount  //
         //--------------------------------------------------------//
        val rdd14 = rdd2.map(x => (x._3.takeWhile(_!=':'), x._17.toFloat)).map { case (date,  amnt) => (date, (amnt)) }.reduceByKey(_ + _).map { case (date,  amnt) => ((date), amnt) }.sortByKey(true)
        val rdd15 = rdd2.map(x => (x._3.takeWhile(_!=':'), x._4.toInt)).map { case (date, passenger) => (date, (passenger)) }.reduceByKey(_ + _).map { case (date, passenger) => ((date), passenger) }.sortByKey(true)

        val rdd16 = rdd2.map(x => (x._3.takeWhile(_!=':'), x._5.toFloat)).map { case (date, distance) => (date, (distance)) }.reduceByKey(_ + _).map { case (date,  distance) => ((date), distance) }.sortByKey(true)

        val rdd17 = rdd2.map(x => (x._3.takeWhile(_!=':'), x._18.toInt)).map { case (date, tripcount) => (date, (tripcount)) }.reduceByKey(_ + _).map { case (date,  tripcount) => ((date), tripcount) }.sortByKey(true)
        val rdd18 = rdd2.map(x => (x._3.takeWhile(_!=':'))).map { case (date) => (date) }
        val amount_df1 = rdd14.toDF("date","amount")
        val rdd19 = rdd14.map(x => (x._1.takeWhile(_!=':'))).map(_.split(" ")).map(row => (row(0), row(1)))

        val realdate1 = amount_df1.select("date").collect().map(_(0)).toArray.map(_.toString)
        val amount1 = amount_df1.select("amount").collect().map(_(0)).toArray.map(_.toString)
        val passenger1 = rdd15.toDF("date","passenger").select("passenger").collect().map(_(0)).toArray.map(_.toString)
        val distance1 = rdd16.toDF("date","distance").select("distance").collect().map(_(0)).toArray.map(_.toString)
        val tripcount1 = rdd17.toDF("date","tripcount").select("tripcount").collect().map(_(0)).toArray.map(_.toString)
        val hour1 = rdd19.toDF("date1","hour").select("hour").collect().map(_(0)).toArray.map(_.toString)

        val date1 =  amount_df1.select("date").collect().map(_(0)).toArray.map(_.toString.trim).map(num => outputFormat.format(inputFormat.parse(num)))

        val taxi_array1 = Array(realdate1, date1, amount1,passenger1,  distance1, tripcount1,hour1).transpose
        val taxi_rdd1 = sc.parallelize(taxi_array1).map(col => Taxi_class1(col(0).toString, col(1).toString, col(2).toDouble, col(3).toInt, col(4).toDouble ,col(5).toInt, col(6).toString ))
        val taxi_df1 = taxi_rdd1.toDF("realdate","date","amount","passenger","distance","tripcount","hour")

        // saving the file to hdfs "hourwise aggregation"
        taxi_df1.coalesce(1).write.format("csv").option("header", "true").mode("append").save("hdfs://quickstart.cloudera/datada/2017_table3.csv")
        sc.stop()
       } // end of 'if' block
      } // end of 'try' clock
    catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Had an IOException trying to read that file") }
        } // end block of 'month' for loop
      } // end block of 'year' for loop

    } // end block for 'main' function
} // end block for 'object'
