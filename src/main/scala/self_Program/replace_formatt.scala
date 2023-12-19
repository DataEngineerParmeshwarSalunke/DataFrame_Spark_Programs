package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, regexp_replace, when}

object replace_formatt {


  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("slunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val data = Seq(
                    (1, "abcd", "30/40/22"),
                    (2, "sachin", "12/22/33")).toDF("id","name","number")
    val new_df = data.withColumn("new_column", regexp_replace(col("number"),"/",","))
    data.show()
    new_df.show()

    val df = List("sachin madke","parma salunke").toDF("names")

    df.withColumn("new_name", regexp_replace(col("names")," ","_")).show()
  }
}
