package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, element_at, regexp_replace, split, sum}

object scenario_based_qu {
  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = List(
      (1, "Customer1", "2023-01-10", 100),
      (2, "Customer2", "2023-01-11", 150),
      (3, "Customer1", "2023-01-12", 80),
      (4, "Customer3", "2023-01-12", 200),
      (5, "Customer2", "2023-01-13", 50)
    ).toDF("order_id", "cust_id", "order_date", "tot_amount")
        //🔹Total Revenue Analysis
       df.agg(sum("tot_amount").as("total_revenue")).show()

   // 🔹Highest Value Order

    val heighest_order = df.orderBy(desc("tot_amount"))
    heighest_order.show(1)

    //🔹Top-Spending Customer
    df.groupBy("cust_id").agg(sum("tot_amount").as("spen_amount"))
                              .orderBy(desc("spen_amount")).show(1)

    //🔹 Order Count per Customer
    df.groupBy("cust_id").agg(count("order_id").as("order"))
      .orderBy(desc("order")).show()

  }
}
