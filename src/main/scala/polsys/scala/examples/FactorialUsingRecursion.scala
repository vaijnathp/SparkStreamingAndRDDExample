package polsys.scala.examples

import org.apache.log4j.Logger

import scala.io.StdIn

object FactorialUsingRecursion {
  val log = new Logger("Factorial")

  def getFactorialByNormalRecursion(number: Int): Int = {
    try{
      if (number == 0) return 1
      number * getFactorialByNormalRecursion(number - 1)
    }
    catch {
      case e:StackOverflowError =>
        throw new StackOverflowError(e.getMessage)
    }
  }

  def getFactorialByTailRecursion(res: Int, number: Int): Int = {
    if (number == 0) return res
    getFactorialByTailRecursion(res * number, number-1)
  }

  def main(args: Array[String]): Unit = {
    try {
      log.info("Please enter the number for Factorial")
      val number = StdIn.readInt()
      log.info("Normal Recursion \n\t Factorial of the Number:"+number+" = "+getFactorialByNormalRecursion(number))
      log.info("Tail Recursion \n\t Factorial of the Number:"+number+" = "+getFactorialByTailRecursion(1, number))
    }
    catch {
      case e:NumberFormatException =>
        log.error("you entered non-numeric value "+e )

      case e:StackOverflowError =>
        log.error("Recursion caused Stack memory full "+e)
    }
  }
}
