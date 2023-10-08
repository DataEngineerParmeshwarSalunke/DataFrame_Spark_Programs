package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}

//Calculate the difference between the current salary and the minimum salary within the last three rows, ordered by id.
object Q6_diffe_sal {
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


     }

}
