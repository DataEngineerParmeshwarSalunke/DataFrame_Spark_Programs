package week1_SparkContext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object count_ipAddress_inter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("C:/Users/dell/Desktop/salunke/ipaddress.txt");

    val words = input.flatMap(x => x.split((",")))

    val words1 = words.map(x => (x, 1))

    val words2 = words1.reduceByKey((x, y) => x + y)
   // val output =  words2.sortByKey(true) //val resultsort=result.sortByKey(false)

    val output =  words2.sortBy(x => x._2, false)
    val ipadd = output.take(2)
    output.collect.foreach(println)
    println("total IP addresses :"+ words2.count())

    println("Top 2 IP addersses : ")

    ipadd.foreach(println)

    println("total IP addresses :"+ words2.count())
    scala.io.StdIn.readLine()

  }
}
