package polsys.hackathon.challanges

import scala.collection.mutable.ListBuffer
import scala.io.StdIn

/**
  * Created by Ramdut on 13-12-2018.
  */

object Result {


  def customSort(arr: Array[Int]) {
    val keyData = arr.toList.map(word => (word, 1))
    val groupedData = keyData.groupBy(_._1)
    val result = groupedData.mapValues(list => {
      list.map(_._2).sum
    })

    val p = result.groupBy(_._2).map(e => e._1 -> e._2.toList.map(e => e._1).sortWith(_ < _)).toSeq
      .sortWith(_._1 < _._1).toList

    p.flatMap(e => {
      val list = ListBuffer[Int]()
      e._2.foreach(ele => (0 until e._1).foreach(_ => list += ele))
      list
    }).foreach(println)
  }

}

object ListSortByFrequencyAndThenValue {
  def main(args: Array[String]) {
    val arrCount = StdIn.readLine.trim.toInt

    val arr = Array.ofDim[Int](arrCount)

    for (i <- 0 until arrCount) {
      val arrItem = StdIn.readLine.trim.toInt
      arr(i) = arrItem
    }

    Result.customSort(arr)
  }
}
