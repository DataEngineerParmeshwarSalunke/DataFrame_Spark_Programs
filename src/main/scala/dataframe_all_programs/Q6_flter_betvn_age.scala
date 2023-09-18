package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
//)Given a DataFrame df, use SQL-like expressions to filter and display the rows where the
//age is between 25 and 40 (inclusive)
object Q6_flter_betvn_age {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((15,"female"),(20,"male"),(30,"male"),(25,"female"),(40,"female"),(50,"male")).toDF("age","gender")

     df.show()
    println("age is between 25 and 40 ")
    df.filter("age >=25  AND age <=40").show()
  }

}
