package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object write_read_data {
  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

     import spark.implicits._
    val data = Seq(  (1,"sachin","20-09-2004"),
                     (2,"mohan","21-03-1990"),
                     (3,"sandeep","15-04-2023"),
                     (4,"deepak","10-01-2008")
                   ).toDF("id","name","dob")

    data.write
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .csv(path = "C:/Users/dell/Desktop/salunke/output/file1")

   val dataframe = spark.read
     .format("csv")
     .option("header",true)
     .option("inferschema",true)
     .option("path", "C:/Users/dell/Desktop/salunke/output/file1")
     .load()

       dataframe.show()
    spark.stop()
  }

}
