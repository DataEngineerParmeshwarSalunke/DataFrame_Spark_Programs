package week1_SparkContext
       //filter out the data which is divisible by 11
       //filter out data which is divsible by 3
       //  3 &&11       3 || 11
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object divisible_program {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[8]", "salunke.com")

    val rdd = sc.parallelize(Array(11, 22, 33, 44, 55, 6, 9, 12, 18, 20, 25, 29))
    val divi11Rdd = rdd.filter(x => x % 11 == 0)
    val div3RDD = rdd.filter(a => a % 3 == 0)

    val div11and3RDD = rdd.filter(b => (b % 3 == 0 && b % 11 == 0))

    val div11or3RDD = rdd.filter(b => (b % 3 == 0 || b % 11 == 0))
     println("Divisible by 11")
    divi11Rdd.collect.foreach(println)
    println("Divisible by 3")
    div3RDD.collect.foreach(println)
    println("Divisible by 11&&3")
    div11and3RDD.collect.foreach(println)
    println("Divisible by 11 || 3")
    div11or3RDD.collect.foreach(println)

    scala.io.StdIn.readLine()

  }
}