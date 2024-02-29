package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, regexp_replace, split}

object explode_data {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("slunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val data = Seq(
      ("jhon", "Cricket Badminton"),
      ("Sam", "Tenis Cricket"),
      ("Peter", "Cricket Football")).toDF( "Name", "Hobbies")
        data.select(
          col("Name"),
          explode(split($"Hobbies","\\s+"))
        ).show()
  }
}