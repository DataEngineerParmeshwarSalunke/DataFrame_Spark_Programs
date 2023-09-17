package week1_SparkContext

import org.apache.spark.SparkContext

object mangotest {
  def main(args: Array[String]): Unit = {
    // Create Spark configuration
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("E:/salunkeprogram/mangotest.txt");

    val words = input.flatMap(x => x.split((" ")))

    val wordsmap = words.map(x => (x, 1))

    val wordcount = wordsmap.reduceByKey((x, y) => x + y)

    println(" input Partition is : " + input.getNumPartitions)
    println("wordcount Partition is : " + wordcount.getNumPartitions)

    wordcount.collect.foreach( println )

     scala.io.StdIn.readLine()
  }
}
