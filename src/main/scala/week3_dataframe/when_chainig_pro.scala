package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object when_chainig_pro {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(("Alice", 25, 1500), ("Bob", 30, 2000), ("Charlie", 35, 3000)).toDF("name", "age",
      "salary")
    val transformedDF = df.select(
      col("name"),
      when(col("age") < 30, "Young").otherwise("Old").as("age_group"),
      when(col("salary") > 2000, col("salary") * 0.9).otherwise(col("salary"))
        .as("discounted_salary")
    )
    transformedDF.show()

       }

}
