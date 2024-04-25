package self_Program

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, desc}

object bankproject {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Banking_Domain").master("local[*]").getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)

    val df1 = spark.read.format("csv")
                        .option("inferschema",true).option("header",true).load("E:/salunkeprogram/accountCV.csv")
    df1.show()
    val df2 = spark.read.format("csv")
                        .option("inferschema", true).option("header", true).load("E:/salunkeprogram/transcactioncv.csv")
    df2.show()

    val extractresult = extractValidTransformation(df1,df2)
    extractresult.show()
    val result = transactionByaccount(df2)

    result.foreach(println)
  }
  def transactionByaccount(transaction_df: DataFrame): Map[String, Long] = {

    val transactionCounts = transaction_df.groupBy("fromAccountNumber")
      .agg(count("*").alias("transactionCount"))

    val top10 = transactionCounts.orderBy(desc("transactionCount"))
      .limit(10)
      .collect()
    val resultMap = top10.map(row => (row.getString(0), row.getLong(1))).sortBy(-_._2).toMap

     resultMap
  }
  def extractValidTransformation(account_df: DataFrame, transaction_df: DataFrame): DataFrame ={

    //join
 val joineddf = transaction_df.join(account_df,account_df("accountNumber")===transaction_df("toaccountNumber"),
                            "inner").filter(transaction_df("transferamount")<= account_df("balance"))
    joineddf
  }
}
