package polsys.structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import scala.util.control.Breaks._


class ThreadExample extends Runnable{
  override def run(): Unit = {
    breakable(
      for (i <- 0 to 5) {
        Thread.sleep(50000)
        if (Checkpointing.query != null) {
          println(Checkpointing.query.id)
          println(Checkpointing.query.status.isDataAvailable)
          if (!Checkpointing.query.status.isDataAvailable) {
            Checkpointing.query.stop()
            break;
          }
        }
      }
    )
  }
}

object Checkpointing {
 var query:StreamingQuery=_;

  def main(args: Array[String]): Unit = {
    var e = new ThreadExample()
    var t = new Thread(e)
    t.setPriority(Thread.MIN_PRIORITY)
    t.start()
    val spark = SparkSession.builder().appName("checkpoint").master("local[*]").getOrCreate()
    val schema=StructType(Seq(StructField("f1",StringType,true)))

    val df=spark.readStream.schema(schema).csv("C:\\Users\\AkashT\\Documents\\testData")

     query=df.groupBy("f1").count().writeStream.format("console")
      .option("checkpointLocation","checkpointLocation").outputMode("complete").start
      t.join()
     query.awaitTermination()


  }
}
