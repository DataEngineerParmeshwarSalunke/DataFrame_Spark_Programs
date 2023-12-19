package week1_SparkContext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object word_Count_tolowerCase {
  def main(args: Array[String]): Unit = {
   // Logger.getLogger("org").setLevel(Level.OFF)
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("E:/salunkeprogram/scenario.txt");

    val words = input.flatMap(x => x.split((" ")))

    val lowercase = words.map(x => x.toUpperCase( ))

    val words1 = lowercase.map(x => (x, 1))

    val words2 = words1.reduceByKey((x, y) => x + y)

   val output =  words2.sortBy(x => x._2, false)
    output.collect.foreach(println)

    scala.io.StdIn.readLine()

  }
}
