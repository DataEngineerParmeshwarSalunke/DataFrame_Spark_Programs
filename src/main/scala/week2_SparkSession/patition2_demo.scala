package week2_SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object patition2_demo {
  def main(ags:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Partition.com").master("local[*]").getOrCreate()
  //  spark.sparkContext.setLogLevel("ERROR")

    val schema = "name String, age Int,salary Int, location String"

    val df = spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .option("path","C:/Users/dell/Desktop/salunke/patition2.csv")
      .load()

    df.show()

    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
     // .partitionBy("location")
      .bucketBy(4,"location")
     // .option("MaxRecordsPerFile",2)
      .option("path","C:/Users/dell/Desktop/salunke/output")
      .saveAsTable("bucket_table")

    scala.io.StdIn.readLine()

  }

}
