import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import WikiUtils._

object TopLinked {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: TopLinked <master-url> <wiki-file>")
      System.exit(1)
    }

    val master = args(0)
    val file = args(1)

    val conf = new SparkConf().setMaster(master).setAppName("TopLinked")
    val sc = new SparkContext(conf)

    val articles = loadArticles(sc, file)
    val links = articles.flatMap(a => a.links)
    val counts = links.groupBy(a => a).mapValues(_.size)
    val max = counts.reduce((a, b) => if (a._2 > b._2) a else b)
    val topArticle = max._1

    println(topArticle)

    sc.stop()
  }
}
