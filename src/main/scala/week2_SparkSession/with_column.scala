package week2_SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object with_column {
  def main(args:Array[String]): Unit={
        Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()

    val df = List(("Alice", 25), ("Bob", 30), ("Charlie", 35),("juli",15),("John",40),("Jac",50))

    val data_df = spark.createDataFrame(df).toDF("name", "age")
    val categorizedDF = data_df.withColumn("category", when(col("age") <= 18, "Young")
                       .when(col("age") <=30,"elder")
                       .otherwise("Old"))

    categorizedDF.show()

   val data_age = data_df.withColumn("age_differance", when(col("age") < 18, "child")
                 .when(col("age") >= 18 and col("age") <= 30,"young")
                 .when(col("age") >= 31 and col("age") <= 40,"elder")
                 .otherwise("adult"))

    data_age.show()
  }

}
