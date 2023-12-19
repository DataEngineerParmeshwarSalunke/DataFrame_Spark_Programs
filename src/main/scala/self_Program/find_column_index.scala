package self_Program

object find_column_index {
  def main(args: Array[String]): Unit = {
    // Define a list of column names
    val columnNames = List("Name", "Age", "City", "Salary")

    print("Enter a column number: ")
    val inputNumber = scala.io.StdIn.readInt()

    if (inputNumber >= 1 && inputNumber <= columnNames.length) {
      val columnName = columnNames(inputNumber - 1)

      println(s"Column $inputNumber corresponds to: $columnName")
    } else {
      println("Invalid column number. Please enter a valid column number.")
    }
  }


}
