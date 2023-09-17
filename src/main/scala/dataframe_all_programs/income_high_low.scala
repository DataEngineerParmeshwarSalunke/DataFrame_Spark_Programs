package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object income_high_low {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List(25000,30000,40000,66000,80000,90000).toDF("income")

    df.select(     col("income"),
         when(col("income") <= 30000,"LOW")
        .when(col("income") > 30000 && col("income") <= 70000,"Medium")
           .when(col("income") > 70000,"High")
           .otherwise("null").as("category")
    ).show()
  }

}
