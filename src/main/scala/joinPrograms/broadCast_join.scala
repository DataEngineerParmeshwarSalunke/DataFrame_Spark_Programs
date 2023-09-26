package joinPrograms

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}
import org.apache.log4j.{Logger,Level}
object broadCast_join {
  def main(args:Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    val df1schema = " id Int, name String, age Int"
    val df2schema = "id Int,name String,dept String"

    val df1 = spark.read
      .format("csv")
      .option("header", true)
      .schema(df1schema)
      //.option("inferschema",true)
      .option("path", "C:/Users/dell/Desktop/salunke/salunkeage.csv")
      .load()
    val df2 = spark.read
      .format("csv")
      .option("header", true)
      .schema(df2schema)
      //.option("inferschema",true)
      .option("path", "C:/Users/dell/Desktop/salunke/sample_join.csv")
      .load()
    df1.show()
    df2.show()
    val conditionType = "inner"

    val condition = df1.col("id") === df2.col("id")

    val joinedDf = df1.join(df2, condition, conditionType)

    joinedDf.show()
    scala.io.StdIn.readLine()
  }
}
