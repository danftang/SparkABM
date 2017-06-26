/**
  * Created by daniel on 13/06/17.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MyFirst {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").appName("My App").getOrCreate()
    val result = MyABM.run()
    result.graph.vertices.collect().foreach(println)
  }
}
