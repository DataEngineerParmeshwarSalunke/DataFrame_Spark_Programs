package scala

object range_palindrome {
  def main(args:Array[String]): Unit={

    var num=0

    for(i<-0 to 100) {
      num = i
      var rev = 0
      var rem = 0
      while (num != 0) {
        rem = num % 10
        rev = rev * 10 + rem
        num = num / 10
      }
      if ( i==rev) {
        print(i+" ")
      }
    }

  }
}
