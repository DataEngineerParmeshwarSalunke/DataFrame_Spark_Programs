package scala

object findnumfromstring {
  def main(args: Array[String]): Unit = {
    def string_integer(num: String): String = {
      var count = 0
      var result = ""
      for (i <- 0 until num.length) {
        if (num(i) == '7') {
//          count += 1
//          if (count == 3) result = "777"
//        } else {
//          count = 0
          print(num(i)+" ")
        }
      }
      result
    }

    val num = "6777133339"
    string_integer(num)


    }
  }

