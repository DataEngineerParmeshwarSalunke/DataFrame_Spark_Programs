package week2_SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, when}

object start_end_char {
  def main(args:Array[String]): Unit={
        Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = List(("Alice", 25), ("Bob", 30), ("Charlie", 35),("juli",15),("John",40),("Jac",50))
      .toDF("name", "age")

      df.filter(col("name").startsWith("A")).show()
       df.filter(col("name").endsWith("i")).show()
      df.select(concat(col("name"),col("age"))).show()

       df.select(    col("name"),
                     col("age"),
         when(col("age") > 30 && col("name").startsWith("J"), "Senior")
        .when(col("age") > 20 && col("name").endsWith("e"), "Mid-Level")
        .otherwise("Junior").as("position")).show()
  }
}
/*
+-------+---+---------+
|   name|age| position|
+-------+---+---------+
|  Alice| 25|Mid-Level|
  |    Bob| 30|   Junior|
  |Charlie| 35|Mid-Level|
  |   juli| 15|   Junior|
  |   John| 40|   Senior|
  |    Jac| 50|   Senior|
  +-------+---+---------+
*/