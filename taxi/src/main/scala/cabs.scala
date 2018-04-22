
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object cabs extends App {
  println("Processing taxi")
  
  // spark://127.0.0.1:7077  
  val conf =  new SparkConf().setAppName("ClusterScore").setMaster("local")//.set("spark.storage.memoryFraction", "1")

   val sc = new SparkContext(conf)
  //val sc = SparkContext.getOrCreate();
  
  val csv = sc.textFile("/media/rahul/WinNTFSData/Study/Big Data/Data_/Data/yellow_tripdata_2016-07.csv");
  // split / clean data
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  // get header
  val header = headerAndRows.first
  header.foreach(println)
  // filter out header (eh. just check if the first val matches the first header name)
  val data = headerAndRows.filter(_(0) != header(0))
  
  data.take(5).foreach(row => println(row.toList) )
    
  val rdd2 = data.map(row => (1, row(1), row(2), row(3)))  
  rdd2.take(5).foreach(row => println(row) )  
  
  //val seqOp = (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1)
  //val combOp = (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
  
  val rdd3 = rdd2.map(x => (x._1, x._4));
  rdd3.take(5).foreach(row => println(row) )
  
  val rdd3_top5 = sc.parallelize(rdd3.take(5).map(row => (row._1.toString(), row._2.toInt)));
  
  rdd3_top5.foreach(println )
  val rdd4 = rdd3_top5.reduceByKey(_+_);
  rdd4.foreach(row => println(row) )
  
  
  // terminate spark context
  sc.stop()
  
}