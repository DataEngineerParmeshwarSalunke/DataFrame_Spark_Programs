package week1_SparkContext

import org.apache.spark.SparkContext

object parallel_Union {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[8]", "salunke")

    val rdd1 = sc.parallelize(Array(1, 2, 3))
    val rdd2 = sc.parallelize(Array(3, 4, 5))
    val unionRdd = rdd1.union(rdd2)
     unionRdd.foreach(println)
      scala.io.StdIn.readLine()

  }
}
