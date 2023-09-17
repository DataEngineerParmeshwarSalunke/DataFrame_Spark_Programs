package scala

object find_two_number {
  def main(args:Array[String]): Unit= {
    val arr = Array(1, 2, 3, 4, 5,7)

    for (i <- 0 until arr.length) {
      if (arr(i) ==4 || arr(i)==7) {
        println("Search element found at position "+i+ " is : = "+ arr(i))
      }
      }
  }

}
