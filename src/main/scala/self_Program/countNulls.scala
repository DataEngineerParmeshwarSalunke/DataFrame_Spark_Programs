package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, concat, count, lit, regexp_replace, sum, when}

object countNulls {
  def main(args:Array[String]): Unit ={
 Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("slunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = Seq(     ("C101","HSBC", 8500),
                      (null,"VISA",""),
                      (null,"Master",50000),
                      ("C104","",70000) ).toDF("Id", "account", "Score")
    df.show()

  }
}
