package week3_dataframe

import org.apache.spark.sql.SparkSession

object drop_null {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = List(("sachin",15),("sandeep",25),("deepak",null),(null,35),("sachin",40))
             .toDF("name","age")


    val nonNullDF = df.na.drop()

    nonNullDF.show()
    }

}
