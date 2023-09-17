package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, element_at, split}

object extract_email_id_2nd {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List("sandeep@gmail.com", "deepak@yahoo.com","karthik@rediff.com","parmeshwar@gmail.com")
                .toDF("email")
    val df_Split = df.withColumn("split_email", split(col("email"), "@"))


    val df_extract = df_Split.withColumn("username", element_at(col("split_email"), 1))
                             .withColumn("domain", element_at(col("split_email"), 2))
       df_extract.show()
  }

}
