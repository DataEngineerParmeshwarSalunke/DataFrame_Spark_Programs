package week3_dataframe

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, element_at}


object split_column {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(("Alice,123", 25), ("Bob,456", 30), ("Charlie,789", 35)).toDF("name", "age")
//    val transformedDF = df.select(   col("name"),
//                                     col("age"),
//      functions.split(col("name"),",").as("new_name"),
//
//      element_at(col("new_name"),1).as("first_value"),
//      element_at(col("new_name"),2).as("2nd_value"))
//     transformedDF.show()
      val splitDF = df.withColumn("split_name", functions.split(col("name"), ","))

     val transformedDF = splitDF.select(
      col("name"),
      col("age"),
      col("split_name"),
      element_at(col("split_name"), 1).as("first_value"),
      element_at(col("split_name"), 2).as("second_value")
    )

    transformedDF.show()
  }
}
