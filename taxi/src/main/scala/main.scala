
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import com.datastax.spark.connector._

// define main method (Spark entry point)
object main {
  def main(args: Array[String]) {
 
    // https://stackoverflow.com/questions/42032169/error-initializing-sparkcontext-a-master-url-must-be-set-in-your-configuration
    //val conf = new SparkConf().setMaster("spark://master")
    // initialise spark context
    //val conf = new SparkConf().setAppName("HelloWorld")
    //val sc = new SparkContext(conf)
    
    // <--- This is what's missing
    // spark://127.0.0.1:7077
    val conf =  new SparkConf().setAppName("ClusterScore").setMaster("local")//.set("spark.storage.memoryFraction", "1")

    val sc = new SparkContext(conf)
    
    val csv = sc.textFile("/home/rahul/Study/Big Data/Preprocess/Weather_2016_2017.csv");
    
    
    csv.foreach(println);
    // split / clean data
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    header.foreach(println)
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    // val maps = data.map(splits => header.zip(splits).toMap)
    // filter out the Raining days
    // val raining = maps.filter(map => map("Rain") != "No")
    // print result
    // println("Raining");
    // raining.foreach(println)
    
    // filter out the Raining days
    // val snowing = maps.filter(map => map("Snow") != "No")
    // print result
    // println("Snowing");
    data.take(5).foreach(row => println(row.toList) )
    
    val rdd2 = data.map(row => (1, row(1), row(2), row(3)))
    
    rdd2.take(5).foreach(row => println(row) )
    
    // Print headers
    header.foreach(println)
    
    // val day = snowing.map( map => (map("Day").replace("-",".") , map("Avg_temperature") , map("Rain") , map("Snow") ))
    // Print headers
    // day.foreach(println)
    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    println("************")
    println("************")
    
    //val database_c = sc.cassandraTable("test" , "kv")
    
    // terminate spark context
    sc.stop()
    
  }
}

/*
 def main(args: Array[String]): Unit = {
      // Read the CSV file
      val csv = sc.textFile("/path/to/your/file.csv")
      // split / clean data
      val headerAndRows = csv.map(line => line.split(",").map(_.trim))
      // get header
      val header = headerAndRows.first
      // filter out header (eh. just check if the first val matches the first header name)
      val data = headerAndRows.filter(_(0) != header(0))
      // splits to map (header/value pairs)
      val maps = data.map(splits => header.zip(splits).toMap)
      // filter out the user "me"
      val result = maps.filter(map => map("user") != "me")
      // print result
      result.foreach(println)
    }
 * /
 */
