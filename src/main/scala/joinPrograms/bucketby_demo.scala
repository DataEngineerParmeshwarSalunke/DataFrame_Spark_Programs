package joinPrograms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import javax.jws.WebParam.Mode

object bucketby_demo {
  def main(args:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()
    println("Start Execution ")

    val df = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/dell/Desktop/SparkInputfiles/employee.txt")
    .load()
    df.show()

    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .partitionBy("address","gender")
      .option("path","C:/Users/dell/Desktop/salunke/output")
      .saveAsTable("part_buck")
     //.save()
    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .bucketBy(4,"id")
      .option("path","C:/Users/dell/Desktop/salunke/output1")
      .saveAsTable("mango")

    println("Execution Completed")
    scala.io.StdIn.readLine()
  }
}
