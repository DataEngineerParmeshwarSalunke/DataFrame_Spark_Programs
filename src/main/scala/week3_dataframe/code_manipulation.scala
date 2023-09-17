package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring, when}

object code_manipulation {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(("Alice", "abc1223"), ("Bob", "deef456"), ("Charlie", "ghii789")).toDF("name", "code")
    val transformedDF = df.select(
                                  col("name"),
                                  col("code"),
        when(col("code").rlike("[0-9]{3}"), substring(col("code"), 1, 4))
        .otherwise(col("code")).as("extracted_code")
    )
    transformedDF.show()
//    df.select(    col("code"),
//           when(col("code").like("[a-z]") , substring(col("code"),1,3))
//             .otherwise(col("code")).as("first_string")).show()

  }

}
