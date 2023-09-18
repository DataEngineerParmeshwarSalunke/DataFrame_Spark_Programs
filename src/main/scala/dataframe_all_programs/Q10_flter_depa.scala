package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, window}
//10)Given a DataFrame df containing employee data with columns employee_id, department,
//and salary, filter and display the departments where the average salary of employees in that
//department is greater than $60,000.
object Q10_flter_depa {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val data_df = List( (1,"HR",20000), (2,"mechanical",30000), (3,"civil",20000),(4,"HR",40000),
                    (5,"mechanical",22000), (6,"civil",50000), (7,"HR",61000),(8,"mechanical",71000),(9,"civil",65000)
                     ).toDF( "employee_id", "department", "salary")

    val result = data_df.groupBy(col("department")).agg(avg("salary").as("avg_salary"))
     .filter(col("avg_salary")>60000)
   // val result2 = result.filter(col("avg_salary") > 20000)
    //result2.show()
    val departmentsDf = result.select("department")
    departmentsDf.show()

    result.show()
  }
}
