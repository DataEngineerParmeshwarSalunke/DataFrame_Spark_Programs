package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}
object parallel_cartesian {
  def main(args:Array[String]): Unit ={
      Logger.getLogger("org").setLevel(Level.FATAL)
    val sc = new SparkContext("local[8]", "salunke")

        val rdd1 = sc.parallelize(Array(1, 2, 3))
        val rdd2 = sc.parallelize(Array("A", "B", "C"))
        val cartesianRdd = rdd1.cartesian(rdd2)
        cartesianRdd.foreach(println)

    scala.io.StdIn.readLine()

  }

}
