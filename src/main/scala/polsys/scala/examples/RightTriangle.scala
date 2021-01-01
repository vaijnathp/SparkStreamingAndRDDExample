package polsys.scala.examples

/**
  * Created by Ramdut on 30-07-2018.
  */
object RightTriangle {


  def main(arrx:Array[String]) : Unit={
    val height=scala.io.StdIn.readInt()
//    triangle1(height)
    triangle2(height)
  }

  private def triangle1(height: Int) = {
    /* *
       **
       ***
       ****
       *****
       ****** */

    for (i <- 0 to height) {
      for (j <- 0 to i) {
        print("*")
      }
      println()
    }
  }
  def triangle2(height: Int): Unit = {
    /* ******
       *****
       ****
       ***
       **
       *  */
    for (i <- 0 to height){
      for (j <- 0 to height-i ){
        print("*")
      }
      println()
    }
  }

}
