/**
  * Created by daniel on 13/06/17.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MyFirst {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").appName("My App").getOrCreate()
    var i = 1
    var result = MyABM.createGraph(10)
    val edges = result.graph.edges.map((tuple) => {tuple}).cache()
    var vertices = result.graph.vertices.map((tuple) => {tuple}).cache()
    for(i <- 1 to 100) {
      result = result.run(-1, MyABM.step, MyABM.send)
      vertices = result.graph.vertices.map((tuple) => {tuple}).cache()
      result = new ABMGraph[Int](vertices, edges)
      println("step "+i)
    }
    result.graph.vertices.collect().foreach(println)
  }
}
