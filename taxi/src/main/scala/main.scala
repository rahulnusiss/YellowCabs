
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
 
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
    
    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    println("************")
    println("************")
    
    // terminate spark context
    sc.stop()
    
  }
}
