package week1_SparkContext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object parallel_Intersection {
  def main(args:Array[String]): Unit ={
      Logger.getLogger("org").setLevel(Level.FATAL)
    val sc = new SparkContext("local[8]", "salunke")

        val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5,8,11,15))
        val rdd2 = sc.parallelize(Array(4, 5, 6, 7, 8,10,12,11,15))
        val intersectionRdd = rdd1.intersection(rdd2)
        intersectionRdd.foreach(println)

    scala.io.StdIn.readLine()

  }

}
