/* pagerank.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object pagerank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("pagerank")
    val sc = new SparkContext(conf)

    val num_iters = if (args.length > 1 ) args(1).toInt else 10;

    val lines = sc.textFile("input.txt").cache()
    val edges = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.cache()

    val nodes = edges.values.max.toFloat   //Nodes Count
    val danglingNodes = edges.values.subtract(edges.keys).distinct()  //Dangling Nodes
    val links = edges.distinct().groupByKey().cache()

    // Initi pagerank mass: 1/nodes
    var ranks = links.mapValues(v => 1.0).union(danglingNodes.map(v => (v, 1.0))).mapValues(v => v/nodes)
    val alpha = 0.1
    val danglingNode = danglingNodes.first
    for ( i <- 1 to num_iters)
    {
      val danglingMass = ranks.filter(_._1 == danglingNode).map(_._2).first 
      val mapped = links.join(ranks).values.flatMap{ case (pages, rank) =>
        val size = pages.size
        pages.map(page => (page, rank/size))}
      ranks = mapped.reduceByKey(_ + _).mapValues(alpha/nodes + (1-alpha) * danglingMass/nodes +  (1-alpha) * _)
    }
    ranks.saveAsTextFile("output")
    sc.stop()
  }
}