package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.doubleToLiteral
import org.apache.log4j.{Level,Logger}
object parallel_filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[8]", "salunke.com")

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))

    val filteredRdd = rdd.filter(x => x % 2 != 0)

    val count = filteredRdd.count()
    println("Count of odd numbers: " + count)
    filteredRdd.collect.foreach(println)
     scala.io.StdIn.readLine()

  }
}
