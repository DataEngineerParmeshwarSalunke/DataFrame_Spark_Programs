package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
object parallel_distinct {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[8]", "salunke")

    val rdd = sc.parallelize(Array(1, 2, 3, 2, 1, 4, 5))
    val distinctRdd = rdd.distinct().sortBy(f => f)
    distinctRdd.foreach(println)
    scala.io.StdIn.readLine()
  }
}
