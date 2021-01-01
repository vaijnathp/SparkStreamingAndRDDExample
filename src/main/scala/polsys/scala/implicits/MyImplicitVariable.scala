package polsys.scala.implicits

/**Scala “implicits” allow you to omit calling methods or referencing variables
 directly but instead rely on the compiler to make the connections for you
 ts
Implicits in Scala refers to either a value that can be passed “automatically”,
so to speak, or a conversion from one type to another that is made automatically*/

class MyImplicitVariable(num:Int) {
  def myPrint():Unit = for(i <- 1 to num) println(i)
}

object MyImplicitVariable{
  implicit def implicitDef(x:Int): MyImplicitVariable = new MyImplicitVariable(x)
  def main(args: Array[String]): Unit = {
    3.myPrint()
  }
}