package week2_SparkSession

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object read_Mode_example {
  def main(args:Array[String]): Unit={
         Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()
            // val localschema = "id Int,name String,age Int"

    val localschema = StructType(List(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)))

    val data = spark.read
                 .format("csv")
                 .option("header",true)
               //  .option("inferschema",true)
                 .schema(localschema)
                 .option("mode", "DROPMALFORMED")  // DROPMALFORMED , FAILFAST, PERMISSIVE
                 .option("Path","C:/Users/dell/Desktop/salunke/salunkeage.csv")
                 .load()

                  data.show(10, false)
                  data.printSchema()

               scala.io.StdIn.readLine()
  }

}
