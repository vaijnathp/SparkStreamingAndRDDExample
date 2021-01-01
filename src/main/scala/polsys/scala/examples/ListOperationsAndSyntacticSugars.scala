package polsys.scala.examples

import scala.collection.immutable
import scala.io.Source

class ListOperationsAndSyntacticSugars {
  private val source = Source.fromFile("src\\main\\resources/Data/employee.csv")
  val list: immutable.Seq[String] = source.mkString.split("[\\|\n]").toList

  def wordCounting(list: immutable.Seq[String]): List[(String, Int)] = {
    val interRes = list.map(e => (e,1)).groupBy(e => e._1)
    interRes.map(e=> (e._1,e._2.map(_._2).sum)).toList.sortBy(_._1)
  }
}

object mainProject{
  def main(args: Array[String]): Unit = {
    val listObj:ListOperationsAndSyntacticSugars = new ListOperationsAndSyntacticSugars()
    val words:List[(String,Int)] = listObj.wordCounting(listObj.list)
    words.foreach(println)
  }
}