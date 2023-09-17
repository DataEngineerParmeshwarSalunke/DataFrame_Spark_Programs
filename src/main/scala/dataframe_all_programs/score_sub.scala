package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object score_sub {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((90,"marathi"),(70,"math"),(75,"science"),(65,"english"),(50,"geography")).toDF("score","sub")

         df.select(   col("score"),
                      col("sub"),
                when(col("score") >= 90 ,"A")
               .when(col("score") >= 70 && col("score") <=89 ,"B")
               .otherwise("C").as("grade")

         )show()
  }

}
