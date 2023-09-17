package scenario_Based_Pro

import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object word_Count {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("E:/salunkeprogram/scenario.txt");

    val words = input.flatMap(x => x.split((" ")))

    val words1 = words.map(x => (x, 1))

    val words2 = words1.reduceByKey((x, y) => x + y)

    val counts = words2.count()

    val top = words2.sortBy(f => f,ascending = false)

    words2.collect().foreach( println )
    println("Tatal words are : " + counts)
    println("Top words are : ")
    top.foreach(println)

    scala.io.StdIn.readLine()

  }
}
