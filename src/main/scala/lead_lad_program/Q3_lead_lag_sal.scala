package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}

//Calculate the lead and lag of the salary column ordered by id
object Q3_lead_lag_sal {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
 val df = List(   (1,"Alice",1000),
                  (2,"Bob",200),
                  (3,"Charlie",1500),
                  (4,"David",3000)
                  ).toDF("id1", "name", "salary")

     val window_Spec = Window.orderBy("id1")
    val lead_Lag = df.withColumn("lead", lead("salary",1).over(window_Spec))
      .withColumn("lag",lag("salary",1).over(window_Spec))
    lead_Lag.show()
     }

}
