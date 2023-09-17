package week2_SparkSession

import org.apache.spark.sql.{SaveMode, SparkSession}

object partition_by_write {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()

    val schema = " name String,age Int,city String"

    val df = spark.read
      .format("csv")
      .option("header", true)
      .schema(schema)
      .option("path", "C:/Users/dell/Desktop/salunke/partition.csv")
      .load()

    df.show()


    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .partitionBy("city")
      .option("path", "C:/Users/dell/Desktop/salunke/output")
      .save()


    scala.io.StdIn.readLine()

  }
}
