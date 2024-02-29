package self_Program

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, element_at, lit, regexp_replace, split, when}

object nullsHandles {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Null_Handle").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferschema", true)
      .option("path", "E:/salunkeprogram/nullrecords.txt")
      .load()

    df.show()

  }
}
