package self_Program

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{Logger,Level}
object extract_url1 {
  def main(args:Array[String]): Unit={
   Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("URLExtractor").master("local[*]")
      .getOrCreate()

    val data = List("https://www.cognizant.com/in/en", "https://www.example.com")

    import spark.implicits._
    val df = data.toDF("url")

    val extractDomain = udf((url: String) => {
      val uri = new java.net.URI(url)
      val host = uri.getHost
      if (host != null) {
        host.split("\\.") (1)  // Extract the second part of the domain
      } else {
        ""
      }
    })

    val result = df.withColumn("domain", extractDomain($"url"))

    result.show(false)
    spark.stop()
  }


}
