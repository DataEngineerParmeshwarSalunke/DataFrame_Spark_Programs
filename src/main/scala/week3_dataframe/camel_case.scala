package week3_dataframe

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, initcap}

object camel_case {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = List(("sachin",15),("sandeep",25),("deepak",30),("rahul",35),("manoj",40))
             .toDF("name","age")
    val camel_df = df.withColumn("name",initcap(col("name")))

    df.select(functions.concat(col("name"),col("age"))).show()
    camel_df.show
  }

}
