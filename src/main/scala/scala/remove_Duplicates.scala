package scala

object remove_Duplicates {
  def main(args:Array[String]): Unit={

    val arr = Array(2,2,20,30,10,50,10,70,15,20)
    val len = arr.length-1

    var temp=0
    var j=0
    for(i<-0 until  len){
      for(j<-0 until  len){

           if(arr(j)>arr(j+1)){
             temp = arr(j)
             arr(j) = arr(j+1)
             arr(j+1)=temp
           }
         }}
    println("sorting array")
      for(k<-0 until  len){
        println(arr(k)+" ")
      }
    println("Remove Duplicates from the array")
    for(k<-0 until len){
      if(arr(k) != arr(k+1)){
        print(arr(k)+" ")
     }
    }
    }
  }

