package windows_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, max, sum}

object window_avg_rating {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("Window_data").master("local[4]").getOrCreate()
      import spark.implicits._
    val ratingData = List(
      ("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)
    ).toDF("User", "Movies", "Rating")
    val windowSpec = Window.partitionBy( col("User")).orderBy(col("Movies"))//    .rowsBetween(-2,0)
    val avgrating = ratingData.withColumn("Average rating", avg("Rating").over(windowSpec))
    avgrating.show()

  }
}
