package scala

object findpair {
  def main(args: Array[String]): Unit = {

    def twoSum(nums: Array[Int], target: Int): Array [Int] = {
      var left = 0
      var right = nums.length - 1
      while (left < right) {
        val sum = nums(left) + nums(right)
        if (sum == target) {
        //  print(left+" "+right)
          return Array(nums(left), nums(right))
        } else if (sum < target) {
          left += 1
        } else {
          right -= 1
        }
      }
      Array.empty[Int] // Return an empty array if no solution is found
    }
    val nums = Array(2, 7, 11, 15)
    val target = 9
    twoSum(nums, target)
   val output = twoSum(nums, target)
    println(output.mkString("[", ",", "]"))
   // println(output)
  }
}
