package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, element_at, initcap, split}

object extract_url {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val inputUrl = "https://www.cognizant.com/in/en"
//    val extractedText = spark.sparkContext.parallelize(Seq(inputUrl))
//      .flatMap(url => url.split("/"))
//      .filter(_.nonEmpty)
//      .foreach(print)
    //println(extractedText)

    val input = spark.sparkContext.parallelize(List(inputUrl))

    val rdd = input.flatMap(x => x.split("."))
    rdd.foreach(x => println(x))
  }

}
