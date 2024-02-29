package scala

object maxcharString {
  def main(args:Array[String]): Unit = {

    val str = "abbccc"
    val arr  = new Array[Int](127)
    val len = str.length
    for(i<- 0 until len){

      arr(str.charAt(i))= arr(str.charAt(i))+1
    }
    var max = 1
    var c = ' '
    for(i<- 0 until len){
      if(max < arr(str.charAt(i))){
        max = arr(str.charAt(i))
        c= str.charAt(i)
      }
    }
  println(s"The maximum character is $c")
  }

}
