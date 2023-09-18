package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.log4j.{Level,Logger}
import org.apache.log4j.{Level,Logger}

object Q1_filter_dsply_1Row {
  def main(args:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((1,"sachin", "sharma"), (2,"deepak", "rathod"), (3,"nitin", "row"))
                               .toDF("emp_id","first_name", "last_name")

    df.filter(col("last_name")==="row").show()

  }

}
