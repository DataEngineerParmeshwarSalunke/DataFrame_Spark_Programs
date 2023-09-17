package scala

object max_min_Number {
  def main(args:Array[String]): Unit={

    val arr = Array(2,20,30,10,50,70,15)
    val len = arr.length-1
    var max=arr(0)
    var min = arr(0)

    for(i<-0 to len){

      if(arr(i)>max){
        max=arr(i)
      }
    }
      println(s"Maximum number of an Array is : $max")

    for (i <- 0 to len) {

      if (arr(i) < min) {
        min= arr(i)
      }
    }
    println(s"Minimum number of anArray is : $min")
  }
}
