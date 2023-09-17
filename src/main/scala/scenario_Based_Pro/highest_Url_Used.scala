package scenario_Based_Pro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object highest_Url_Used {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("E:/salunkeprogram/highesturl.txt");

    val words = input.flatMap(x => x.split((" ")))
    val lower = words.map(x => x.toLowerCase())
    val words1 = lower.map(x => (x, 1))

    val words2 = words1.reduceByKey((x, y) => x + y)
    val counts = words2.count()
    val top10url = words2.sortBy(x=> x._2,false).take(5)
    println("All URLs")
    words2.collect().foreach( println )
    println("Total URLs are : " + counts)
    println("Top 5 URLs are : ")
    for (elem <- top10url) { print(elem+ " ")}
    scala.io.StdIn.readLine()

  }
}
