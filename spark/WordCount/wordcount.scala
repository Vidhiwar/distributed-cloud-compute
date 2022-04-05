/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
	def main(args: Array[String]) {

	val logfile = "/usr/local/spark/README.md"

	val conf = new SparkConf().setAppName("Simple Application")
    	val sc = new SparkContext(conf)

    	val rdd_text = sc.textFile("input.txt")
    	val rdd_words = rdd_text.flatMap(x => x.split(" "))
    	val rdd_map = rdd_words.map(x => (x.toLowerCase(),1))
    	val rdd_reduce = rdd_map.reduceByKey(_+_)
    	rdd_reduce.saveAsTextFile("output")
    	println(rdd_reduce.collect())
    	sc.stop()



	}
}
