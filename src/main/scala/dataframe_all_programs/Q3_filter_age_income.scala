package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
//Given a DataFrame df containing customer data with columns age and income, filter and
//display the rows where the age is greater than 30 and the income is greater than $50,000
object Q3_filter_age_income {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((20,25000),(20,30000),(30,40000),(60,66000),(66,80000)).toDF("age","income")

       df.show()
    println("display the rows where the age is greater than 30 and the income is greater than $50,000")

    df.filter(col("age")>30 && col("income")>50000).show()
  }

}
