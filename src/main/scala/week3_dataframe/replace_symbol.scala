package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, when}

object replace_symbol {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(("Alice, 123 | 456", 25), ("Bob, 789", 30), ("Charlie | 321", 35)).toDF("name", "age")
    val replace_df = df.withColumn("name", regexp_replace(col("name"), "\\s|,|\\|", ""))
                       .withColumn("newdata", regexp_replace(col("age"), "\\|,|-",""))
    replace_df.show()

  }
}
