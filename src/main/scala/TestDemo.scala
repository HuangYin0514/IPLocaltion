object TestDemo {

  def testR(): Int = {
    var i = 0
    while (i < 20) {
      val my: AnyVal = if (i % 2 == 0) {
        i
      }
      i += 1
      println(s"while in $my")
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val myR = testR()
    println(myR)
  }

}
