package week2_SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object read_sampledata_file {
  def main(args:Array[String]): Unit={
       Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()

    val data = spark.read
                 .format("csv")
                 .option("header",true)
                .option("inferschema",true)
                 .option("Path","C:/Users/dell/Desktop/salunke/sampledata11.txt")
                 .load()

                  data.show(30, false)
                  data.printSchema()

               scala.io.StdIn.readLine()
  }

}
