package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, element_at, split, when}

object extract_email_id {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List("sandeep@gmai.com", "deepak@yahoo.com","karthik@rediff.com","parmeshwar@gmail.com")
                .toDF("email")

    val result = df.withColumn("split_email", split(col("email"),"@"))
      .withColumn("username",element_at(col("split_email"),1))
      .withColumn("domain", element_at(col("split_email"),2))

       result.show(10,false)

//    df.select(  col("email") ,
//        split(col("email"), "@").as("split_email"),
//        element_at(col("split_email"),1).as("username"),
//        element_at(col("split_email"),2).as("domain")
//
//    ).show()

  }

}
