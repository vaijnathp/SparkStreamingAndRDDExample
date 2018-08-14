package polsys.structuredStreaming

import org.apache.spark.TaskContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerStreamingStarted}
import org.apache.spark.util.LongAccumulator

/**
  * Created by vaijnathp on 7/2/2018.
  */
object ExecutionStatus {
  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    sparkSession.sparkContext.addSparkListener(new SPListener)
    import sparkSession.implicits._
    val accum=new LongAccumulator()
    sparkSession.sparkContext.register(accum,"VaijnathAccumulator")
    val lines=sparkSession.readStream.format("socket").option("host","bhsen").option("port","9999").load()
   val df=lines.mapPartitions(itr=> {
     itr.map(r=> {
       accum.add(1)
       r
     })
    })(RowEncoder(lines.schema))


    val words=df.as[String].flatMap(_.split(" "))

    val wordCounts=words.groupBy("value").count()

    val console=wordCounts.writeStream.format("console").outputMode(OutputMode.Complete()).start()

    sparkSession.streams.addListener(new StreamListener)
    console.awaitTermination()

  }
}

class StreamListener extends  StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println("Started the query Processing")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println("Query in process")
  }
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println("Query Executed Successfully")
  }

}
class SPListener extends SparkListener{
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { println("          Task Started*****")}
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { taskEnd.taskInfo.accumulables.foreach(a=> if (a.name.get.equals("VaijnathAccumulator"))println("Name: "+a.name+" Value:"+a.value))
    println("          Task Ended*****")}
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {println("   Stage Started*****") }
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {println("   Stage Ended*****") }
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { println("Job Started*****")}
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { println("Job Ended*****")}
}

