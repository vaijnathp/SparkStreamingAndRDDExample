package polsys.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by vaijnathp on 3/10/2018.
  */
object SocketListening {


  def main(args: Array[String]): Unit = {
    val sc=new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val dStreamContext=new StreamingContext(sc,Seconds(3))

    val stream = dStreamContext.socketTextStream("bhsen",9999)

    val flatMap=stream.flatMap(_.split(" "))

    val words=flatMap.map(w=> (w,3))

    val wordCounts=words.reduceByKey(_+_)

    wordCounts.print()

    dStreamContext.start()
    dStreamContext.awaitTermination()

  }
}
