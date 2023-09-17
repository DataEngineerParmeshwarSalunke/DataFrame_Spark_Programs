package scala

object sum_Digit {
  def main(args:Array[String]): Unit={
    println("Enter the Number")
    var num = scala.io.StdIn.readInt()
    var sum =0
    var rem = 0

    while (num != 0){
      rem = num%10
      sum = sum+ rem
      num= num/10
    }
     println(s"Sum of Digit: $sum")
  }
}
