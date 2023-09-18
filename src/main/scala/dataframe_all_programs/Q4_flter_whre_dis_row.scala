package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.log4j.{Level,Logger}
//Given a DataFrame df containing transaction data with columns transaction_id,
//product_id, and quantity, filter and display the rows where the product_id is in the list of
//products to track (e.g., [101, 102, 103]). Use both the filter and where methods for filtering.
object Q4_flter_whre_dis_row {
  def main(args:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((101,10,"1263nhd"),(102,20,"1633fgd"),(107,0,"2865fhf"),(104,25,"2902asd"),(109,5,"1238ajh")
      ,(106,5,"12877hdh")).toDF("product_id","quantity","transaction_id")

    val productsToTrack = List(101, 102, 103,109)
      df.show()
    println("product_id is in the list of products to track (e.g., [101, 102, 103])")
    df.filter(col("product_id").isin(productsToTrack:_*)).show()

    df.where(col("product_id").isin(productsToTrack:_*)).show()

  }
}
