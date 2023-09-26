package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
object display_negative {


  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

     import spark.implicits._
    val df = Seq(1,2,3,4,-5).toDF("value")

  val result = df.withColumn("negative_value ",
    when(col("value") > 0, -col("value")).otherwise(col("value")))
  //  .withColumn("positive_valu",
     // when(col("value")<0 , +col("value")).otherwise(col("value")))
    result.show()
  }
}
