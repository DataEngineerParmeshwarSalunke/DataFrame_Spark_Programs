package scala

object maxStringFind {
  def main(args:Array[String]): Unit = {

    def maxstring(str : Array[String]) : String ={
       if(str.isEmpty){
         "empty"
       } else{
         str.maxBy(_.length)
       }
    }
    val str1 = Array("apple", "banana", "orangee", "kiwi", "grape")
    val result = maxstring(str1)
    println(s"The maximum String is  $result")
  }
}
