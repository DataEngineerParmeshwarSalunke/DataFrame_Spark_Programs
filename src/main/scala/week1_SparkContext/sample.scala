package week1_SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object sample {
  def main(args: Array[String]): Unit = {
     var s="good"
    // Create Spark configuration
    val sc = new SparkContext("local[8]", "salunke")

    val input = sc.textFile("E:/salunkeprogram/samplespark.txt");

    val words = input.flatMap(x => x.split((" ")))

    val wordsmap = words.map(x => (x, 1))

    val wordcount = wordsmap.reduceByKey((x, y) => x + y)

   // words.collect.foreach( println )
    wordcount.collect.foreach( println )
    scala.io.StdIn.readLine()

  }
}
