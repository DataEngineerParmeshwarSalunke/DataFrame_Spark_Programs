package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.log4j.{Level,Logger}
object when_interview_batch {
  def main(args:Array[String]):Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = List(("john", 25, "Hyd"), ("mohan", 56, "mum"), ("meera", 67, "AP")).toDF("name", "age", "city")

    df.select(col("name"), col("age")
      , when(col("age") > 20 && col("age") < 30, "ADULT")
        .otherwise("OLD").as("category")).show()

  val df2=List(("john",25,"Hyd"),("mohan",56,"mum"),("meera",67,"AP"),("meher",89,"Hyd")).toDF("name","marks","city")

    df2.select(   col("name"),  col("marks"),    col("city"),
      when(col("marks")<=30,"Fail")
      .when(col("marks")> 30 && col("marks")<= 60,"Pass")
      .when(col("marks")>60,"First_class")
      .otherwise("no match").as("Records")
    ).show()

    val df3 = List(60, 30, 40)
      .toDF("Number")


    df3.select(col("Number")
      , when(col("Number") >= 60 && col("Number") >= 30
        && col("Number") >= 40, "Largest Number")
        .otherwise("smallest Number")
        .as("Large OR Small")).show()


    scala.io.StdIn.readLine()

  }

}
