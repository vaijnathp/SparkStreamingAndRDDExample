package polsys.scala.examples

import scala.collection.mutable.ListBuffer

/**
  * Created by Ramdut on 30-07-2018.
  */
object RemoveDups {
def main (arrx:Array[String]):Unit={

  val string: String = scala.io.StdIn.readLine()
  val trimmed: String = string.substring(1,string.size-1)
  val array=trimmed.split(",").map(e => e.toInt)
  val list=new ListBuffer[Int]()

  for (ele <- array){
   if(!list.contains(ele)) {
     list+=ele
   }}
    print("{")
    print(list.mkString(",")+"}")
  }

}

