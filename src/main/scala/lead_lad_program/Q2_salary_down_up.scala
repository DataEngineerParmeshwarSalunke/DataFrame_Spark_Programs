package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}

//If salary is less than previous month we will mark it as "DOWN", if salary has increased then "UP"
object Q2_salary_down_up {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
 val df = List(    (1, "John",1000, "01 / 01 / 2016"),
                   (1,"John", 2000, "02 / 01 / 2016"),
                   (1,"John", 1000, "03 / 01 / 2016"),
                   (1,"John", 2000, "04 / 01 / 2016"),
                   (1,"John", 3000, "05 / 01 / 2016"),
                   (1, "John", 1000, "06 / 01 / 2016")
                   ).toDF("ID", "NAME", "SALARY","DATE")

    val windowSpec = Window.partitionBy("NAME").orderBy("DATE")

    val downUp = df.withColumn("PREVIOUS_SALARY", lag("SALARY", 1).over(windowSpec))
      .withColumn("DOWN_UP", when(col("SALARY") < col("PREVIOUS_SALARY"), "DOWN")
        .when(col("SALARY") > col("PREVIOUS_SALARY"), "UP")
        .otherwise(""))
      .drop("PREVIOUS_SALARY")
       downUp.show()
     }

}
