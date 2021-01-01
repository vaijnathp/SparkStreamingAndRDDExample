package polsys.scala.implicits

/**
  * an implicit class. If you have a class whose constructor takes a single argument,
  * as above, then it can be marked as implicit and the compiler will automatically allow
  * implicit conversions from the type of its constructor argument to the type of the class.
  *
  * implicit class should be inner class
  */
class MyImplicitClass{
  implicit class ImplicitClass(x: Int) {
    def printNTimes():Unit = {
      for( i <- 1 to x) { println("Hello"+i)}
    }
  }
}

object MyImplicitClass{
  def main(args: Array[String]): Unit = {
    val myImplicitClass: MyImplicitClass = new MyImplicitClass()
    import myImplicitClass.ImplicitClass
    3.printNTimes()
  }
}


