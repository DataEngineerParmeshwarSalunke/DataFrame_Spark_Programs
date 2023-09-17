package scala

object find_PairsOfSum {
  def main(args: Array[String]): Unit = {
    var s=' '
    do {
      val arr = Array(1, 2, 3, 5, 6, 8, 10, 11) //array must be in distinct and sorted
      val len = arr.length - 1
      println("Enter the summation value to print 2 paris of given summation")
      val sum = scala.io.StdIn.readInt()
      var low = 0
      var high = len
      while (low < high) {
        if (arr(low) + arr(high) > sum) {
          high = high - 1
        } else if (arr(low) + arr(high) < sum) {
          low = low + 1
        } else if (arr(low) + arr(high) == sum) {
          println("summation pairs are : " + arr(low) + "," + arr(high) + " =" + sum)
          low = low + 1
          high = high - 1
        }
      }
      println("There are no pairs of summation value")
      println("Do you want to continue press 'y' otherwise press 'n' ")
      s=scala.io.StdIn.readChar()
    }while(s=='y' || s=='Y')
  }
}
