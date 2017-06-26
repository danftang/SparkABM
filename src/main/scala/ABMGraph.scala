import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, Edge}

import scala.reflect.ClassTag

/**
  * Created by daniel on 15/06/17.
  *
  * Base class for creating ABMs on top of Spark/GraphX.
  *
  * To use it, create an instance of an ABMGraph by constructing it with a
  * set of nodes (agents) and a set of edges (communication channels between agents).
  * The node set should be an RDD of 2-tuples consisting of (VertexId, AgentState),
  * and the edge set should be an RDD of graphx.Edge[Boolean]s.
  *
  * Once you've created your graph, set maxIter to the maximum number of
  * iterations you want to do, then call run().
  *
  * run takes a step function and a send function (defined below) and returns the
  * state of the graph at the end of the simulation.
  *
  * The step function is called for each agent at each time-step. When called,
  * the agent's Id, state any incoming messages (from the last step) are passed in.
  * The function should return the agent's new state.
  *
  * The send function is called for each edge, which takes an EdgeTriplet and should
  * return a list of messages to send from the source to the destination of the edge.
  *
  * Under the hood is the GraphX implementation of the Pregel algorithm,
  * see https://kowshik.github.io/JPregel/pregel_paper.pdf
  *
  */
class ABMGraph[STATE](g: Graph[STATE,Boolean])(implicit evidence$1 : ClassTag[STATE]) {
  val graph : Graph[STATE,Boolean] = g
  var maxIter : Int = 5;

  def this(vertices : org.apache.spark.rdd.RDD[(VertexId, STATE)],
           edges : org.apache.spark.rdd.RDD[Edge[Boolean]])(implicit evidence$1 : ClassTag[STATE]) = this(Graph[STATE,Boolean](vertices,edges))

  def run[MESSAGE](initialMessage : MESSAGE,
          step : (VertexId,STATE,List[MESSAGE]) => STATE,
          send : (EdgeTriplet[STATE,Boolean]) => List[MESSAGE])
         (implicit  evidence$2 : ClassTag[List[MESSAGE]]) : ABMGraph[STATE] = {
    new ABMGraph[STATE](graph.pregel[List[MESSAGE]]( List(initialMessage), maxIter)(
      step,
      (triplet) => {
        val messages = send(triplet)
        if(messages.isEmpty)
          Iterator.empty
        else
          Iterator[(VertexId,List[MESSAGE])]((triplet.dstId,messages))
      },
      (a,b) => a ::: b
    ))
  }
}
