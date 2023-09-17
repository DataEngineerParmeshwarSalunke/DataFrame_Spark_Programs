package week1_SparkContext

import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object parallel_Search_Item {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)

    val sc = new SparkContext("local[8]", "salunke")

    val rdd1 = sc.parallelize(Array("apple","grape","banana","orange","pear","grape"))
    val searchitem = "grape"

    val item = rdd1.filter(x => x==searchitem)
      println("matched context")
      item.collect.foreach(println)
      println("Substring contains")

      val itemsearch = "an"
      val itemcontains = rdd1.filter(x => x.contains(itemsearch))
      itemcontains.collect.foreach(println)
      scala.io.StdIn.readLine()

  }
}
