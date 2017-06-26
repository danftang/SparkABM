import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

/**
  * Created by daniel on 13/06/17.
  */
class PingPongGraphFrames {
  def run(): Unit = {
    val spark = SparkSession.builder().master("local").appName("My App").getOrCreate()

    val N = 10;
    var v = new Array[(Int,Int)](N)
    var e = new Array[(Int,Int)](N)
    for(i <- 0  to N-1) {
      v(i) = (i,i%2)
      e(i) = (i,i + 1 - 2*(i%2))
    }
    //    var vert = spark.createDataFrame(v).withColumnRenamed("_1","id").withColumnRenamed("_2","count")
    var vert = spark.createDataFrame(v).toDF("id","count")
    var edge = spark.createDataFrame(e).toDF("src","dst")
    val am = AggregateMessages
    var graph = GraphFrame(vert, edge)
    //    vert.show()

    // cache graph

    graph.vertices.cache().count()
    graph.edges.cache().count()
    val edges = am.getCachedDataFrame(graph.edges)
    edges.count()

    // iterate
    //    for (i <- 1 to 1) {
    val msgs = graph.aggregateMessages.sendToDst(am.src("count")).agg(sum(am.msg).as("count"))
    val cachedNewVertices = am.getCachedDataFrame(msgs)
    graph = GraphFrame(cachedNewVertices, edges)
    //    }

    // action

    graph.vertices.show(100)

  }

}
