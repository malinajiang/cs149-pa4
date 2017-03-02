import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import WikiUtils._
import org.apache.spark.storage.StorageLevel._

object PageRank {
  val D = 0.85  // Damping factor in PageRank

  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    if (args.length != 2) {
      System.err.println("Usage: PageRank <master-url> <wiki-file>")
      System.exit(1)
    }

    val master = args(0)
    val file = args(1)

    val conf = new SparkConf().setMaster(master).setAppName("PageRank")
    val sc = new SparkContext(conf)

    val articles = loadArticles(sc, file)

    println(articles.partitions.size)
    println("hello")

    val numArticles = articles.count

    val links = articles.map(a => (a.title, a.links.distinct.filter(b => b != a.title)))
    var ranks = articles.map(a => (a.title, 1.0 / numArticles))

    links.persist(MEMORY_AND_DISK)

    val const = (1.0 - D) / numArticles

    for (i <- 1 to 5) {
      val separateRanks = ranks.join(links).flatMap(x => x._2._2.map(y => (y, x._2._1 / x._2._2.size)))
      val groupedRanks = separateRanks.groupBy(x => x._1).map(group => (group._1, group._2.reduce((a, b) => (a._1, a._2 + b._2))))
      val updatedRanks = groupedRanks.map(x => (x._1, const + D * x._2._2))
      ranks = updatedRanks
    }

    val noCategory = ranks.filter(x => !(x._1.contains(":")))

    val topN = noCategory.top(20)(new Ordering[(String, Double)] {
      override def compare(a: (String, Double), b: (String, Double)) = a._2.compare(b._2)
    }).map(x => x._1)

    val t1 = System.nanoTime()

    topN.foreach(println)
    println("Elapsed time: " + (t1 - t0) + "ns")

    sc.stop()
  }

}
