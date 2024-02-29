package self_Program

import javafx.beans.binding.Bindings.when
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, column, count, desc, sum, to_date}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object student_percentage {
  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val df1 = List((1, "stave"),
                  (2, "david"),
                   (3, "aryan")
                ).toDF("id", "name")

  val  schema1 = List("id", "subject","mark")
    val data = List( (1, "pyspark", 90),
                    (1, "sql", 100),
                    (2, "sql", 70),
                    (2, "pyspark", 60),
                    (3, "sql", 30),
                    (3, "pyspark", 20))
    val df2 = data.toDF(schema1:_*)

       df1.show()
       df2.show()
    val join_df = df1.join(df2,df1.col("id")===df2.col("id"))
    val per_df = join_df.groupBy("id","name").agg(sum(col("mark")/ count(col("subject")
                    .as("percentage"))))
//    per_df.show()
//    val result_df = per_df.withColumn("Result",
//                     when(col("percentage") >= 80, "Distinction")
//                      .when(col("percentage")>=60 && col("percentage") < 80 ,"First_class")
//                      .otherwise("Fail"))
//    result_df.show()
  }
}
