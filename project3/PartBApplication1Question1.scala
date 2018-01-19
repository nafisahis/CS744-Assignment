import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Application1 {

  def main(args: Array[String]) {

    val spark = SparkSession
    .builder
    .config("spark.mastero", "spark://10.254.0.172:7077")
    .config("spark.app.name", "CS-744-Assisgnment2-PartB-App1")
    .config("spark.driver.memory", "8g")
    .config("spark.eventlog.enabled", "true")
    .config("spark.eventLog.dir","file:///home/ubuntu/logs/apps_spark")
    .config("spark.executor.memory", "16g")
    .config("spark.executor.cores","4")
    .config("spark.task.cpus", "1")
    .config("spark.default.parallelism", "16")
    .getOrCreate()

    val sc = spark.sparkContext

    val loadedGraph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, args(0))
    val inputGraph: Graph[Int, Int] = loadedGraph.outerJoinVertices(loadedGraph.outDegrees)(
      (vid, _, degOpt) => degOpt.getOrElse(0)
    )

    var outputGraph: Graph[(Double, Int), Double] = inputGraph.mapTriplets(
      triplet => 0.0
    ).mapVertices((vid, outDegree) => (1.0, outDegree))

    val iters = 20
    for(i <- 1 to iters) {
      outputGraph = outputGraph.mapTriplets(
        triplet => triplet.srcAttr._1/triplet.srcAttr._2
      )

      var contribs: RDD[(VertexId, Double)] = outputGraph.edges.map { (edge) =>
        (edge.dstId, edge.attr)
      }.groupByKey().map { case(vid, newRanks) =>
        (vid, newRanks.sum)
      }

      outputGraph = outputGraph.joinVertices(contribs)(
        (vid, vAttr, contrib) => (0.15 + 0.85*contrib, vAttr._2)
      )
    }

    val output = outputGraph.vertices.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2._1 + "."))
    sc.stop()
  }
}
