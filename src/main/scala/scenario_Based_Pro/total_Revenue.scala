package scenario_Based_Pro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object total_Revenue {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[8]", "salunke")

    val rdd = sc.parallelize(Array(10,20,30,40,50,60,70,80,90,100))

    val revenue = rdd.reduce(_+_)

    println("Total Revenue = "+ revenue)
    scala.io.StdIn.readLine()

  }
}
