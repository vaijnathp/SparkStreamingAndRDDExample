package polsys.scalaExamples

import scala.io.Source

/**
  * Created by vaijnathp on 3/11/2018.
  */
object scalaFileInput {


  def main(args: Array[String]): Unit = {

    val source = Source.fromFile("/Users/vaijnathp/Desktop/tmp/T.csv")

    val words = source.getLines().flatMap(e => e.split(","))

    //val wordsTuple=words.map((_,1))

    val wordCounts = words.foldLeft(Map.empty[String, Int]) {
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }
    println(wordCounts.mkString("\t"))
  }
}
