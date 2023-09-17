package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object age_income {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((20,25000),(20,30000),(30,40000),(60,66000),(66,80000)).toDF("age","income")

         df.select(   col("age"),
                      col("income"),
                when(col("age") <=30 ,"Young")
               .when(col("age") > 30 && col("age") <=60 ,"Senior")
               .otherwise("Unknown").as("age_group")

         )show()
  }

}
