package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.doubleToLiteral

object scnario_Program {
  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val sc = new SparkContext("local[8]", "salunke")

    val rdd = sc.parallelize(Array(10,20,30,40,50,60,70,80,90,100))

    val sumRdd = rdd.reduce(_+_)

    sumRdd.foreach(println)

    scala.io.StdIn.readLine()

  }
}
