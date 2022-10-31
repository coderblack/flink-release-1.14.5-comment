object Xx {

  def main(args: Array[String]): Unit = {

    val lst = Array(1, 2, 3, 4)

    lst.iterator.map({
      println("haha")
      _ * 10
    })


  }

}
