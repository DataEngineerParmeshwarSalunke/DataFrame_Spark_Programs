package scala

object reverse_String {
  def main(args:Array[String]): Unit={
    println("Enter the String")
    val str = scala.io.StdIn.readLine()

    val chararr = str.toCharArray()
    val len = chararr.length-1
    for(i<-len to 0 by -1){
      print(chararr(i))
    }
    println(" ")
    val str1 = "hello"
    val len1 = str1.length-1
    for (i <- len to 0 by -1) {
      print(str.charAt(i))
    }

  }}

