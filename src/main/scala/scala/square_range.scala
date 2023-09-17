package scala

object square_range {
  def main(args:Array[String]): Unit={

   val add = (x : Int, y : Int) => x+y

    println(add(10,20))

    def square(x:Int): Int = x*x;

    for(i<-1 to 10){
      println(s"Square Of Number : $i = "+ square(i))
    }
  }
}
