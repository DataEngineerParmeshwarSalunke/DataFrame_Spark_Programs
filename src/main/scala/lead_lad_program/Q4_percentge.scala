package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, when}

//Calculate the percentage change in salary from the previous row to the current row, ordered by id
object Q4_percentge {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
 val df = List(   (1,"Alice",1000),
                  (2,"Bob",200),
                  (3,"Charlie",1500),
                  (4,"David",3000)
                  ).toDF("id1", "name", "salary")

     val windowSpec = Window.orderBy("id1")
    val leadLag = df.withColumn("lag_salary", lag("salary", 1).over(windowSpec))

      .withColumn("percentage_change", when(col("lag_salary").isNotNull,
        ((col("salary") - col("lag_salary")) / col("lag_salary")) * 100).otherwise(null))

    leadLag.show()
     }

}
