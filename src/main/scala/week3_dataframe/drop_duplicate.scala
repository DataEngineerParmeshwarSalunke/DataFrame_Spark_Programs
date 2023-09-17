package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap}

object drop_duplicate {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = List(("sachin",15),("sandeep",25),("deepak",30),("rahul",35),("sachin",15))
             .toDF("name","age")

    val drop_duplicate = df.dropDuplicates()
    drop_duplicate.show()
    }

}
