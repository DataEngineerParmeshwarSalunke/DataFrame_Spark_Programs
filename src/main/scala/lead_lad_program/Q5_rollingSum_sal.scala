package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, lag, lit, when}

//Calculate the percentage change in salary from the previous row to the current row, ordered by id
object Q5_rollingSum_sal {
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

//      val rolling_Sum = df.withColumn("lag1_sal",lag("salary",1).over(windowSpec))
//        .withColumn("lag2_sal",lag("salary",2).over(windowSpec))
//        .withColumn("Rolling_Sum",col("salary")+col("lag1_sal")+col("lag2_sal"))
//         rolling_Sum.show()

    val rollingSum = df.withColumn("lag1_salary", lag("salary", 1).over(windowSpec))
      .withColumn("lag2_salary", lag("salary", 2).over(windowSpec))
      .withColumn("rolling_sum_salary", col("salary") + coalesce(col("lag1_salary"),
                                               lit(0)) + coalesce(col("lag2_salary"), lit(0)))
      //.drop("lag1_salary", "lag2_salary")

    rollingSum.show()
     }

}
