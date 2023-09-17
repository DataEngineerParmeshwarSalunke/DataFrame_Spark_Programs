package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object filter_daf {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = List(("sachin",15),("sandeep",25),("deepak",30),("rahul",35),("sachin",40))
             .toDF("name","age")

    df.select(    col("name"),
                                 col("age"),
                     when(col("age") < 30,"young")
                    .otherwise("elder").as("category")).show()
   df.select(  col("name").startsWith("s")).show()

       }

}
