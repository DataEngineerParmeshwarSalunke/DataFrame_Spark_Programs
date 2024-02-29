package week1_SparkContext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object  count_ipAddress {
  def main(args: Array[String]): Unit = {
   // Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("E:/salunkeprogram/ipaddress.txt");

    val words = input.flatMap(x => x.split((" ")))

    val lowercase = words.map(x => x.toLowerCase())

    val words1 = lowercase.map(x => (x, 1))

    val words2 = words1.reduceByKey((x, y) => x + y)
   // val output =  words2.sortByKey(true) //val resultsort=result.sortByKey(false)

    val output =  words2.sortBy(x => x._2, false)
    val ipadd = output.take(2)
    output.collect.foreach(println)

    println("Top 5 IP addersses : ")

    ipadd.foreach(println)

    println("total IP addresses :"+ words2.count())
    scala.io.StdIn.readLine()

  }
}
