package week2_SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object read_sparksession {
  def main(args:Array[String]): Unit={
        Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()

    case class employee(first_name: String, last_name: String, email: String, gender: String,
                        ip_address: String,
                        date: String)

    val dataf = spark.read
                 .format("csv")
                 .option("header",true)
                 .option("inferschema",true)
                 .option("Path","C:/Users/dell/Desktop/salunke/sample1.csv")
                 .load()

    dataf.show(10, false)
    dataf.printSchema()

    val dfrdd = dataf.rdd             // convert dataframe into rdd

   import spark.implicits._         //convert dataframe to dataset bu using case class name employee
//    val dt=dataf.as[employee]         // employee has all schema of dataframe data

   // val data1 = dfrdd.toDF()
    println("The no. of partitions are : "+ dfrdd.getNumPartitions )

               scala.io.StdIn.readLine()
  }

}
