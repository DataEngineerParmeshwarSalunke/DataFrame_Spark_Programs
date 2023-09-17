package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object sales_expense_profit {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((1,10,50),(10,0,200),(20,5,100),(-1,50,-1),(5,5,0)).toDF( "sales", "expenses", "profit")

    df.select(     col("sales"),
                   col("expenses"),
                   col("profit") ,
            when(col("profit") > 0 ,"Profitable")
           .when(col("profit")===0,"Break-even")
           .when(col("profit") < 0 && col("sales") > 0,"Loss-making")
           .otherwise("No Sale").as("profit_status")
    ).show()
  }
}
