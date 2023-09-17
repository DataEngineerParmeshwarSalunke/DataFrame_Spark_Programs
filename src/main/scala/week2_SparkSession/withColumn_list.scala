package week2_SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object withColumn_list {
  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("OOMExample").master("local[*]").getOrCreate()

    val mylist = List(("john", 35), ("meer", 45), ("priya", 65))

    val df = spark.createDataFrame(mylist).toDF("name", "age")
   // val df2 = df.toDF("name", "age")
    val df3 = df.withColumn("double_age",col("age")*2)
    val df4 = df3.withColumnRenamed("double_age", "adult")

    df.select("name").show()
    df.show()
    df3.show()
    df4.show()
  }
}
