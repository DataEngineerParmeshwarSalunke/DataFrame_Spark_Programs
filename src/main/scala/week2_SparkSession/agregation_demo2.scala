package week2_SparkSession

import org.apache.spark.sql.{SparkSession, functions}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level,Logger}
object agregation_demo2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("OrderDataExample").master("local").getOrCreate()

    import spark.implicits._

    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")
    scoreData.show()


    import org.apache.spark.sql.functions._
    val average_score = scoreData.groupBy("Subject")
      .agg(avg("Score").alias("Avg_student"))

    val max_score = scoreData.groupBy("Student")
      .agg(functions.max("Score").alias("Score_of_student"))

    val min_score = scoreData.groupBy("Student")
      .agg(functions.min("Score").alias("Score_of_student"))
    println("average score for each subject")
    average_score.show()
    println("the maximum score for each student")
    max_score.show()
    println("the minimum score for each student")
    min_score.show()
 /*   IF score >=
    90 print distinction
    if score is >
    75 && <
    90 then he is firstclass
    if <
    75 then put him under second
    class  */
    scoreData.select(  col("Student"),
      col("Subject"),
      col("Score"),
      when(col("Score")>= 90,"Distinction")
        .when(col("Score")>= 75 && col("Score") < 90 ,"Firstclass")
        .when(col("Score") < 75,"under Second")
        .otherwise("No Match").as("Student class")
    ).show()
  }
}
