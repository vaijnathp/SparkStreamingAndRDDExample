package polsys.hackerearth.challanges


object HashMapUsingLinkedList {
  def main(args: Array[String]): Unit = {
    val map:Map[String,Int] = Map()
    val myHashMap: MyHashMap = MyHashMap(("",""))
  }

}

case class Element(mapElement: (String, String)) {
  var key:String = mapElement._1
  var value: String = mapElement._2
}

case class MyHashMap(tuple: (String, String))