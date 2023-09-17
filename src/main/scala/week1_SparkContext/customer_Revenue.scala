package week1_SparkContext
import org.apache.spark.SparkContext
import org.apache.directory.shared.kerberos.codec.adKdcIssued.actions.StoreIRealm

object customer_Revenue {
  def main(ags:Array[String]): Unit ={
    val sc = new SparkContext("local[10]","P-Salunke")


//    val customer_revenue_rdd = orders_rdd.map(x => (x._2, x._4))
//      .reduceByKey(_ + _)
//
//
//    customer_revenue_rdd.collect().foreach { case (customer_id, revenue) =>
//      println(s"Customer ID: $customer_id, Total Revenue: $revenue")
//    }

  }

}
