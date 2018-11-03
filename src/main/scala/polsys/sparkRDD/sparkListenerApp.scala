package polsys.sparkRDD

import java.util

import org.apache.spark.scheduler._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SaveMode, SparkSession}

object sparkListenerApp {

  def main(args:Array[String]):Unit ={

    val sparkSession = SparkSession.builder().master("local[*]").appName("SparkListener").getOrCreate()
    sparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

        println("Job Started: "+jobStart.jobId+"\n")
        println(jobStart.stageInfos.foreach(f =>f.parentIds.foreach(println)))
      }
new util.HashMap()
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        println("Stage Submitted: "+stageSubmitted)
        stageSubmitted.stageInfo.parentIds.foreach(println)
      }

      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        println("task started: "+ taskStart.taskInfo)
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        println("task Ended: "+ taskEnd.taskInfo)
      }

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit ={
        println("Stage Completed: "+stageCompleted.stageInfo)
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        println("Job End: "+jobEnd.jobId)
      }
    })

    val df = sparkSession.readStream.textFile("input")
    df.writeStream.outputMode(OutputMode.Append()).format("text").option("path","vaij/out").option("checkpointLocation","vaij/checkpoint").queryName("vaijnath").start().awaitTermination()


  }

}


