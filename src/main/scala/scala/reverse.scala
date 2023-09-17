package scala

object reverse {
  def main(args:Array[String]): Unit={
    println("Enter the Number")
    var num = scala.io.StdIn.readInt()
    var rev =0
    var rem = 0
    val checknumber=num
    while (num != 0){
      rem = num%10
      rev = rev*10+rem
      num = num/10

    }
    println(s"Reverse number is : $rev" )

    if(checknumber==rev){

      println(s"This is Palindrome number : "+ num)
    } else{
      println("This is not Palindrome number : "+ num)

    }

  }
}
