package self_Program

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
case class user(user_id:Int, user_name: String, user_city: String)

object create_data_frame {
  def main(args:Array[String]): Unit={
   val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()
    println("Application Start")

   spark.sparkContext.setLogLevel("ERROR")
     println("Approach 1 : ")
    val user_list1 = List((1,"John","London"),
                         (2,"Martin","New York"),
                         (3,"Sam","Sydney"),
                         (4,"Alan","maxico"),
                         (5,"Jacob","florida")
    )
      val user_df = spark.createDataFrame(user_list1)   //.toDF("user_id","user_name","user_city")
      val df_schema = List("user_id","user_name","user_city")
     //val usr_rdd = spark.sparkContext.parallelize(user_list)
     //  val usr_df = spark.createDataFrame(usr_rdd)
     val user_df1 = user_df.toDF(df_schema:_*)
        user_df1.show(5,false)

         println("Approach 2 : ")

     val user_list2 = Seq(Row(1,"John","London"),
                        Row(2,"Martin","New York"),
                        Row(3,"Sam","Sydney"),
                        Row(4,"Alan","maxico"),
                        Row(5,"Jacob","florida")
                        )
      val user_schema = StructType(Array(
        StructField("user_id", IntegerType,true) ,
        StructField("user_name", StringType,true),
        StructField("user_city", StringType,true)
      ))

    val user_dff = spark.createDataFrame(spark.sparkContext.parallelize(user_list2),user_schema)
    user_dff.show()
    println("Approach 3 : ")

  //  case class user(user_id:Int, user_name: String, user_city:  String)

    val user_seq_list3 = Seq(user(1,"John","London"),
                        user(2,"Martin","New York"),
                        user(3,"Sam","Sydney"),
                        user(4,"Alan","maxico"),
                        user(5,"Jacob","florida") )

    val case_user_rdd = spark.sparkContext.parallelize(user_seq_list3)
    val case_user_df = spark.createDataFrame(case_user_rdd)
    case_user_df.show(5,false)
    println("Application Completed")
  }
}
