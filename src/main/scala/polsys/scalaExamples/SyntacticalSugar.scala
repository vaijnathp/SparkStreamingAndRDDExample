package polsys.scalaExamples

/**
  * Created by vaijnathp on 3/23/2018.
  */
object SyntacticalSugar {
  def main(args: Array[String]): Unit = {
    //val tuple6 = (123,"asdf",1.33,"!@#",BigDecimal(38,13))

    val xs = List(10, 20)
    val ys = List(50, 40)
    val xys1 = xs.filter(x => x % 2 == 0).flatMap(x => ys.map(y => (x, y)))


    val xys2 = for {x <- xs
                    if x % 2 == 0
                    y <- ys} yield (x, y)

    println("Xys1 and Xys2 will produce same result :)")
    xys1.foreach(println)
    for (xy <- xys2) yield println(xy)

    //Named Parameters
    val r1 = Range(1, 10, 2)
    val r2=Range(start=1,end = 10,step = 2)
    println("r1"+r1+"\nr2"+r2)

    //an arbitrary number of parameters (of the same type)
    def average(x: Int, xs: Int*): Double =
    (x :: xs.to[List]).sum.toDouble / (xs.size + 1)

    average(1)
    average(1, 2)
    average(1, 2, 3)

    //TYPE ALIASES

    type Result = Either[String, (Int, Int)]
    def divide(dividend: Int, divisor: Int): Result =
      if (divisor == 0) Left("Division by zero")
      else Right((dividend / divisor, dividend % divisor))
    divide(6, 4) == Right((1, 2))
    divide(2, 0) == Left("Division by zero")
//    divide(8, 4) ==

  }
}
