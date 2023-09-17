package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object age_gender {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((15,"female"),(20,"male"),(30,"male"),(25,"female")).toDF("age","gender")

         df.select(   col("age"),
                      col("gender"),
              when(col("age") >= 18 ,"true")
             .otherwise("false").as("is_adult")

         )show()
  }

}
