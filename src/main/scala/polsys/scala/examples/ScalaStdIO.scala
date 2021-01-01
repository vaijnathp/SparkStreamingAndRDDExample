package polsys.scala.examples

import java.text.SimpleDateFormat
import java.util.Date

import scala.io.{Source, StdIn}

/**
  * Created by Ramdut on 28-07-2018.
  */
class ScalaStdIO(val inputInteger:Int, val inputStrings:String,val inputDates: Date) {
  private[this] var _inputInt: Int = inputInteger
  private[this] var _inputString: String = inputStrings
  private[this] var _inputDate: Date = inputDates

  private def inputInt: Int = _inputInt

  private def inputInt_=(value: Int): Unit = {
    _inputInt = value
  }

  private def inputString: String = _inputString

  private def inputString_=(value: String): Unit = {
    _inputString = value
  }

  private def inputDate: Date = _inputDate

  private def inputDate_=(value: Date): Unit = {
    _inputDate = value
  }


}
object ScalaStdIO{
  def main(arr:Array[String]):Unit={
    println("Please Enter the Integer \t String \t Date")
    val simpleDateFormat =new SimpleDateFormat("dd-MM-yyyy")
    val scalaStdIO:ScalaStdIO =new ScalaStdIO(StdIn.readInt(),StdIn.readLine(),simpleDateFormat.parse(StdIn.readLine()))

    println("scalaStdIO.inputInt"+ scalaStdIO.inputInt)
    println("scalaStdIO.inputString"+scalaStdIO.inputString)
    println("scalaStdIO.inputDate"+scalaStdIO.inputDate)

  }
}