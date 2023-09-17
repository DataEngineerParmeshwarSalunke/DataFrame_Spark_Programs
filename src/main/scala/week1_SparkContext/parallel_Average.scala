package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.doubleToLiteral

object parallel_Average {
  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val sc = new SparkContext("local[8]", "salunke")

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
    val sum = rdd.reduce(_ + _)
    val count = rdd.count()
    val average = sum / count.toDouble
    println("Average value: " + average)
        scala.io.StdIn.readLine()

  }
}
