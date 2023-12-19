package self_Program

object find_column_index2 {
  def main(args: Array[String]): Unit = {
    // Define a list of column names
    val columnNames = List("Name", "Age", "City", "Salary")

    print("Enter a column name: ")
    val input_col_name = scala.io.StdIn.readLine()

    val column_index = columnNames.indexOf(input_col_name)

    if(column_index >= 0){

      println(s"column   $input_col_name  is on index = $column_index  ")
    }
    else{
      println("Wrong column")
    }


  }


}
