package dataframe_all_programs
import org.apache.log4j.{Level,Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
//Given a DataFrame df containing product data with columns product_id and product_name,
//filter and display the rows where the product_name is not null
object filter_IsNotNull {
  def main(args:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((101,"mobile"),(102,"laptop"),(103,null),(104,"computer"),(105,null),(106,"television"),(107,"charger")
                 ,(108,null)).toDF("product_id","product_name")

    df.show()
    println(" display the rows where the product_name is not null")
    df.filter(col("product_name").isNotNull).show()
    println(" display the rows where the product_name is null")
    df.filter(col("product_name").isNull).show()


  }
}
