package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.doubleToLiteral

object parallel_Join {
  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val sc = new SparkContext("local[8]", "salunke")

    val rdd1 = sc.parallelize(Array((1, "apple"), (2, "banana"), (3,
      "orange")))
    val rdd2 = sc.parallelize(Array((1, "red"), (2, "yellow"), (4,
      "green")))
    val joinedRdd = rdd1.join(rdd2)
    joinedRdd.foreach(println)

        scala.io.StdIn.readLine()

  }
}
