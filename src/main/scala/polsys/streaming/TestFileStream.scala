package polsys.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by vaijnathp on 3/10/2018.
  */
object TestFileStream {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf=new SparkConf().setAppName("TextFileStream").setMaster("local[*]")

    val streamingContext:StreamingContext=new StreamingContext(sparkConf,Seconds(2))

    val dStream:DStream[String] = streamingContext.textFileStream("C:\\Users\\vaijnathp\\Desktop\\temp")

    val words=dStream.flatMap(_.split(","))

    val tupleWords=words.map(w=> (w,1))

    tupleWords.print()

    val wordCount= tupleWords.reduceByKey(_+_)

    wordCount.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
