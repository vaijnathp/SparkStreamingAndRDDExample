package polsys.hackerearth.challanges

import scala.collection.immutable.HashMap

object HashMapUsingLinkedList {
  def main(args: Array[String]): Unit = {
    val myHashMap: MyHashMap = MyHashMap(("",""))
  }

}

case class Element(mapElement: (String, String)) {
  var key:String = mapElement._1
  var value: String = mapElement._2
}

case class MyHashMap(tuple: (String, String))