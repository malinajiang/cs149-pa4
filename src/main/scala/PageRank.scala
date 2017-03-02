import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import WikiUtils._

object PageRank {
  val D = 0.85  // Damping factor in PageRank

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: PageRank <master-url> <wiki-file>")
      System.exit(1)
    }

    val master = args(0)
    val file = args(1)

    val conf = new SparkConf().setMaster(master).setAppName("PageRank")
    val sc = new SparkContext(conf)

    /** YOUR CODE HERE **/
    // Feel free to import other Spark libraries as needed.
    val articles = loadArticles(sc, file)
    val numArticles = articles.count

    println("num articles: " + numArticles)

    val linkedArticles = articles.map(a => (a.title, a.links.distinct.filter(b => b != a.title)))

    println("linked articles:")
    linkedArticles.collect().foreach(println)

    val ranks = articles.map(a => (a.title, 1.0/numArticles))

    println("ranks:")
    ranks.collect().foreach(println)

    sc.stop()
  }
}
