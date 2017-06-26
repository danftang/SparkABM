import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}

/**
  * Created by daniel on 15/06/17.
  */
object MyABM {
  def run(): ABMGraph[Int] = {
    createGraph(10).run(-1, step, send)
  }

  def createGraph(N : Int): ABMGraph[Int] = {
    val v = new Array[(VertexId,Int)](N)
    val e = new Array[Edge[Boolean]](N)
    for(i <- 0  to N-1) {
      v(i) = (i,i%2)
      e(i) = Edge(srcId=i, dstId = i + 1 - 2*(i%2), false)
    }
    val sc = SparkContext.getOrCreate()

    val vert = sc.makeRDD[(VertexId,Int)](v)
    val edge = sc.makeRDD[Edge[Boolean]](e)

    new ABMGraph(vert,edge)
  }

  def step(id: VertexId, state: Int, inBox: List[Int]): Int = {
    if(inBox.head == -1) state else inBox.head
  }

  def send(triplet: EdgeTriplet[Int, Boolean]): List[Int] = {
    List(triplet.srcAttr)
  }

}
