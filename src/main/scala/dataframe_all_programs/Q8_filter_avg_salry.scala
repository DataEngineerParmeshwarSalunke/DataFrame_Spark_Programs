package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, when}
//Given a DataFrame df containing employee data with columns employee_id, salary, and
//department, filter and display the rows where the salary is greater than the average salary of
//employees in the "Engineering" department
object Q8_filter_avg_salry {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val emp_df = List( (111,1000,"HR"),  (112,1000,"Engineering"),  (113,2000,"HR"),  (114,3000,"Engineering"),
                      (115,4000,"HR"),   (116,2200,"Engineering"),  (113,5000,"HR")
                     ).toDF( "employee_id", "salary", "department")
    emp_df.show()
      val df = emp_df
   val eng_avg_salary = emp_df.filter(col("department") === "Engineering").agg(avg("salary"))
                            .as("avg_salary")
                            .first()
                            .getDouble(0)
    df.filter(col("department") === "Engineering").agg(avg("salary"))show()
    
    val result = emp_df.filter(col("salary") > eng_avg_salary )
    result.show()
  }
}
