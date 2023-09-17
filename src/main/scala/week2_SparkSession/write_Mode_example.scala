package week2_SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
object write_Mode_example {
  def main(args:Array[String]): Unit={
      Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()

    val data = spark.read
                 .format("csv")
                 .option("header",true)
                 .option("inferschema",true)
                 .option("Path","C:/Users/dell/Desktop/salunke/sample22.csv")
                 .load()

                  data.show(30, false)
                  data.printSchema()
    data.write
      .format("csv")
      .mode(SaveMode.Append)   //
      .option("path","C:/Users/dell/Desktop/salunke/output")
      .save()
               scala.io.StdIn.readLine()
  }

}
