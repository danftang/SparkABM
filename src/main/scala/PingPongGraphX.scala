import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/**
  * Created by daniel on 13/06/17.
  */
class PingPongGraphX() {


  //////////////////////////////////////////////////////////////////////////////
  def run() : Unit = {
    var graph : Graph[Int,Boolean] = createGraph()
    val maxIter : Int = 5;
//    graph.vertices.collect.foreach(println)
    println(graph.vertices.count())
    val result = graph.pregel[Int](-1, maxIter)(

      // Agent timestep (receive messages)
      (id, state, message) => {
        // return new state
        if(message == -1) state else message
      },

      // message send
      triplet => {
        // return an iterator to a container of (VertexId, message) tuples
        // ot Iterator.empty to deactivate this vertex
        Iterator((triplet.dstId, triplet.srcAttr))
      },

      // Merge Message
      (a, b) => {
        a + b
      }

    )
    println(result.vertices.count())
//    result.vertices.collect.foreach(println)
  }


  //////////////////////////////////////////////////////////////////////////////
  def createGraph() : Graph[Int,Boolean] = {
    val N = 4000000;//4000000;
    var v = new Array[(VertexId,Int)](N)
    var e = new Array[Edge[Boolean]](N)
    for(i <- 0  to N-1) {
      v(i) = (i,i%2)
      e(i) = Edge(srcId=i, dstId = i + 1 - 2*(i%2), false)
    }
    val sc = SparkContext.getOrCreate()

    val vert = sc.makeRDD[(VertexId,Int)](v)
    val edge = sc.makeRDD[Edge[Boolean]](e)

    Graph[Int,Boolean](vert, edge)
  }


  //////////////////////////////////////////////////////////////////////////////
  def loadGraph(nodeFilename : String, edgeFilename : String) : Graph[Int,Boolean] = {
    var spark = SparkSession.builder().getOrCreate()

    val nodeSchema = new StructType().add("id", LongType).add("val", IntegerType)
    val edgeSchema = new StructType().add("src", LongType).add("dst", LongType)
    val vert = spark.read.schema(nodeSchema).csv(nodeFilename)
      .rdd.map(
      (row) => (row.getAs[VertexId]("id"), row.getAs[Int]("val"))
    )
    val edge = spark.read.schema(edgeSchema).csv(edgeFilename)
      .rdd.map(
      (row) => Edge[Boolean](row.getAs[VertexId]("src"), row.getAs[VertexId]("dst"),true)
    )

    Graph[Int,Boolean](vert, edge)
  }


  //////////////////////////////////////////////////////////////////////////////
  def makeGraphFiles(filenamePrefix : String, N : Long) : Unit = {
    val nodeFile = new PrintWriter(new File(filenamePrefix+"nodes.csv" ))
    var i : Long = 0
    while(i < N) {
      nodeFile.println(i+","+i%2)
      i += 1
    }
    nodeFile.close()

    val edgeFile = new PrintWriter(new File(filenamePrefix+"edges.csv" ))
    i = 0
    while(i < N) {
      edgeFile.println(i+","+(i + 1 - 2*(i%2)))
      i += 1
    }
    edgeFile.close()
  }

}
