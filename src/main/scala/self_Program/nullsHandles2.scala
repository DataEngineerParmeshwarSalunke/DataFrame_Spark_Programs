package self_Program

import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_replace, trim, when}

object nullsHandles2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Null_Handle").master("local[*]").getOrCreate()

    val df = spark.read.format("csv")
      .option("header", true)
      .option("inferschema", true)
      .option("path", "E:/salunkeprogram/nullrecords.txt")
      .load()

    df.show()
    val dfCleaned = df
      .withColumn("first", when(col("first").isin("#", "$", "&", "NA"), lit(null))
        .otherwise(col("first")))
      .withColumn("last", when(col("last").isin("#", "$", "&", "NA"), lit(null))
        .otherwise(col("last")))
      .withColumn("age", when(col("age").isin("#", "$", "&", "NA"), lit(null))
        .otherwise(col("age")))
      .withColumn("salary", when(col("salary").isin("#", "$", "&", "NA"), lit(null))
        .otherwise(col("salary")))
    dfCleaned.show()
    val dfCleanedFinal = dfCleaned
      .withColumn("first", when(trim(col("first")) === "", lit(null))
        .otherwise(col("first")))
      .withColumn("last", when(trim(col("last")) === "", lit(null))
        .otherwise(col("last")))
      .withColumn("age", when(trim(col("age")) === "", lit(null))
        .otherwise(col("age")))
      .withColumn("salary", when(trim(col("salary")) === "", lit(null))
        .otherwise(col("salary")))
    dfCleanedFinal.show()

    df.select(col("age") , regexp_replace(col("age"),null,"'-'")) .show()
    val nullValues = Seq("$", "#", "&", "NA")

  }
}
