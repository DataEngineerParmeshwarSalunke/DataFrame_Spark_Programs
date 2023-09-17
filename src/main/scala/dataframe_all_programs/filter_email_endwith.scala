package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, element_at, split}
import org.apache.log4j.{Level,Logger}
//Given a DataFrame df containing customer data with a column email, filter and display the
//rows where the email ends with "@example.com."
object filter_email_endwith {
  def main(args:Array[String]): Unit={
     Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((1110,"sandeep@gmail.com"), (1231,"deepak@yahoo.com"),(3632,"karthik@rediff.com"),(5322,"parmeshwar@gmail.com")
      ,(3212,"shivani@gmail.com")).toDF("user_id","email")
    df.show()
    println(" the email ends with @gmail.com.")
    df.filter(col("email").endsWith("@gmail.com")).show()

  }

}
