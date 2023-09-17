package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object temp_humidity {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List(("2022-09-11",-1,85),("2023-03-01",2,60),("2018-05-22",10,50),("2024-02-10 ",35,40),("2024-02-10 ",11,40))
                  .toDF("timestamp","temp","hmdt")

    df.select(     col("timestamp"),
                   col("temp"),
                   col("hmdt"),
            when(col("temp") < 0 && col("hmdt") > 80,"Extreme Cold and Humid")
           .when(col("temp") > 0 && col("temp") <=10 && col("hmdt") >= 50
                                          && col("hmdt") <=80,"Cold and Moderate Humidity")
           . when(col("temp") > 10 && col("hmdt") < 50,"Warm and Dry")
           .otherwise("other").as("weather")
    ).show()
  }

}
