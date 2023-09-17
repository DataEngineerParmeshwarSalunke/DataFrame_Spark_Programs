package week1_SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object pall_Demo {
  def main(args:Array[String]): Unit ={

    val sc = new SparkContext("local[8]", "salunke")

    val data = Array(1,2,3,4,5,6,7,8,9)

     val dataRDD = sc.parallelize(data)

     println("first element of rdd is " + dataRDD.first())

    val element = dataRDD.take(5)

      element.foreach(println)
    scala.io.StdIn.readLine()

  }

}
