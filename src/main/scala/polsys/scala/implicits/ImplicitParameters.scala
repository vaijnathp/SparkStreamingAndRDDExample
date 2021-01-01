package polsys.scala.implicits

class ImplicitParameters {
  def sum(a:Int)(implicit b:Int):Int = {
    a+b
  }
}

object ImplicitParameters{
  def main(args: Array[String]): Unit = {
    val implicitParameters = new ImplicitParameters()
    implicit val p: Int = -2
    println(implicitParameters.sum(10))
    A.a(10,20)
  }
}

object A {
  def a(p:Int, q:Int): Unit ={
    print(p+q)
  }
}