package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.doubleToLiteral
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
object parallel_subtract {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[8]", "salunke")

    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Array(3, 4, 5,6,7,8,9))
    val subtractedRdd = rdd2.subtract(rdd1)

        subtractedRdd.foreach(println)
        scala.io.StdIn.readLine()

  }
}
