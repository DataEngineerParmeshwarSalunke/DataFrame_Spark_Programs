package joinPrograms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object join_tables {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val customer_df = List(  (1, "manish", "patna", "30-05-2022"),
                               (2, "vikash", "kolkata", "12-03-2023"),
                               (3, "nikita", "delhi", "25-06-2023"),
                               (4, "rahul", "ranchi", "24-03-2023"),
                               (5, "mahesh", "jaipur", "22-03-2023"),
                               (6, "prantosh", "kolkata", "18-10-2022"),
                               (7, "raman", "patna", "30-12-2022"),
                               (8, "prakash", "ranchi", "24-02-2023"),
                               (9, "ragini", "kolkata", "03-03-2023"),
                               (10, "raushan", "jaipur", "05-02-2023")
                           ).toDF("customer_id", "customer_name", "address", "date_of_joining")

    val sales_df = List(    (1,22,10,"01-06-2022"),
                              (1,27,5,"03-02-2023"),
                              (2,5,3,"01-06-2023"),
                              (5,22,1,"22-03-2023"),
                              (7,22,4,"03-02-2023"),
                              (9,5,6,"03-03-2023"),
                              (2,1,12,"15-06-2023"),
                              (1,56,2,"25-06-2023"),
                              (5,12,5,"15-04-2023"),
                              (11,12,76,"12-03-2023")
                          ).toDF("customer_id","product_id","quantity","date_of_purchase")

    val product_df = List(  (1, "fanta",20),
                              (2,"dew",22),
                              (5,"sprite",40),
                              (7,"redbull",100),
                              (12,"mazza",45),
                              (22,"coke",27),
                              (25,"limca",21),
                              (27,"pepsi",14),
                              (56,"sting",10)).toDF("id","name","price")
    customer_df.show()
    sales_df.show()
    product_df.show()

    val join_df = customer_df.join(sales_df,customer_df("customer_id")===sales_df("customer_id"),
                          "inner").select(sales_df("product_id")).sort("product_id")
    join_df.show()
  }
}