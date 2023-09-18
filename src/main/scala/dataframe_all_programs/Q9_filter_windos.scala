package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, window}

//9)Given a DataFrame df containing sales data with columns order_id, product_id,
//order_date, and quantity, filter and display the rows where the quantity is greater than the
//average quantity for the corresponding product_id over a specified window of time (e.g., the
//last 7 days). Use window functions for this task
object Q9_filter_windos {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val data_df = List( (1,111,"2023-02-10",4), (2,222,"2022-05-15",6), (3,333,"2018-07-03",7), (4,444,"2024-10-20",10),
                      (5,555,"2020-03-22",6), (6,666,"2022-01-09",3), (7,777,"2023-05-22",1)
                     ).toDF( "order_id", "product_id", "order_date","quantity")
    val avg_qun = data_df
    val dff_avg = avg_qun.agg(avg("quantity"))
    dff_avg.show()
    val avg_quantity =data_df.agg(avg("quantity")).first().getDouble(0)

    val result = data_df.filter(col("quantity") > avg_quantity)
    result.show()

    val windowSpec = window(col("order_date"), "7 days")

    val avgQuantityDf = data_df.groupBy(col("product_id"), windowSpec)
      .agg(avg(col("quantity")).as("avg_quantity"))

    val filteredDf = data_df.join(avgQuantityDf, Seq("product_id", "window"), "inner")
      .filter(col("quantity") > col("avg_quantity"))
      .select(data_df.columns.map(col): _*)

    filteredDf.show()

  }
}
