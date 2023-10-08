package joinPrograms

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object demo_filter {
def main(args:Array[String]):Unit= {
  val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("OFF")
  import spark.implicits._
  val df = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35)).toDF("name", "age")
  val filteredDF = df.filter(col("age") > 30)
  filteredDF.show()
  val df1 = Seq(("Alice", "Engineer"), ("Bob", "Developer"), ("Charlie", "Manager")).toDF("name", "role")

  val result = df1.filter(col("role").contains("er"))
  result.show()
  //an is present or not
}
}
