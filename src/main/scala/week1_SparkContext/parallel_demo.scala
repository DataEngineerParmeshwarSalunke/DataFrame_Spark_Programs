package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.doubleToLiteral
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level , Logger}
object parallel_demo {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.WARN)
    val sc = new SparkContext("local[8]", "salunke")

        val arr = Array(1, 2, 3, 4, 5, 6, 3)

        val input = sc.parallelize(arr)

        val rdd2 = input.mean()

        rdd2.foreach(println)
        scala.io.StdIn.readLine()

  }
}
