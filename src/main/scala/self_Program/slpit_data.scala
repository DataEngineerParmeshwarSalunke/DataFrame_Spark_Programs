package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, element_at, regexp_replace, split}

object slpit_data {
  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("slunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val data = List(
                    (1, "12##34##56"),
                    (2, "23##45##56")).toDF("id","raw_data")
    val new_df = data.withColumn("splited_col", split(col("raw_data"),"##"))
    new_df.withColumn("math",element_at(col("splited_col"),1))
      .withColumn("sci",element_at(col("splited_col"),2))
      .withColumn("geo",element_at(col("splited_col"),3)).show()

    new_df.select( col("id"),
      col("splited_col").getItem(1).as("new_math")).show()

    data.show()
    new_df.show()

    data.withColumn("new_name", regexp_replace(col("raw_data"),"##",",")).show()
  }
}
