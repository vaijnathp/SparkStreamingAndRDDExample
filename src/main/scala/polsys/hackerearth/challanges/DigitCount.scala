package polsys.hackerearth.challanges

object DigitCount {
    def main(str:Array[String]):Unit ={
      val name = scala.io.StdIn.readLine()
      var map:Map[Char,Int] = Map()
      for(i <- 0 to 9){
        val c = i.toChar + '0'
        print("********"+c)
//        map = map + ( c -> 0 )
      }
      for(i <- 0 until name.length()){
        val chare = name(i)
        if (chare.isDigit) {
          val res = map.getOrElse(chare,0)
          map = map +(chare ->  (res + 1) )
        }
      }
      map.keys.foreach(e => println(e+" "+map(e)))
    }
  }

