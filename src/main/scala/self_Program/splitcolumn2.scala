package self_Program

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object splitcolumn2 {

  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder.appName("splitdata").master("local[4]").getOrCreate()
    import spark.implicits._
    val inputData = Seq(
      ("nanaware, deepak", "~|", 36),
      ("patole, sachin", "~|", 38),
      ("maindad, nitin", " ~| ", 34),
      ("abc,cde", "~|", 23),
      ("xyz,abc", "~|", 90)).toDF("Name", "delimiter", "Age")
    inputData.show()
    val splitData = inputData
      .withColumn("splitData", functions.split(col("Name"), ","))
      .select(col("splitData").getItem(0).as("Name"), col("splitData").getItem(1)
        .as("Sir_Name"), col("age"))


    splitData.show()

//    val outputData = splitData.write
//      .mode("overwrite").option("header", true).csv("C:/Users/Ganesh/Downloads/src_files/outputFile.csv")

  }

}
