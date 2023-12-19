package scala

object split_string {
  def main(args:Array[String]): Unit= {

    val str = "mango is so sweet"

    val str1 = str.split(" ")
    val len = str1.length-1
    str1.foreach(s => print(s + " "))
    println()
    str1.reverse.foreach(s => print(s+" "))
    println()
    for(i <- str1.length-1 until(0) by -1){

      print(str1(i) + " ")
    }
  }
}
