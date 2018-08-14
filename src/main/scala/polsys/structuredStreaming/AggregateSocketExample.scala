package polsys.structuredStreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by vaijnathp on 5/30/2018.
  */
object AggregateSocketExample {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    import sparkSession.implicits._

    val csvDF = sparkSession.readStream.format("socket").option("host", "bhsen").option("port", "9999").load()


    val countDF = csvDF.as[String].map(line => {
      val arr = line.split(",")
      (arr(0), Timestamp.valueOf(arr(1)))})
      .as[(String, Timestamp)].toDF("name", "timestamp").withWatermark("timestamp","5 seconds")
      .groupBy(window(col("timestamp"),"10 seconds","10 seconds","3 seconds"), col("name")).count()//.withColumn("t1",window(current_timestamp(),"10 seconds"))


    /*.withWatermark("timestamp", "10 seconds")*/

    val console1 =countDF.writeStream.format("console")
      .option("truncate", value = false).outputMode("append").start()
    console1.awaitTermination()


//    FileOutput
    /* val console= countDF.writeStream.format("json")
       .option("checkpointLocation", "vaij")
       .option("path","vaijnath")
       .outputMode(OutputMode.Complete()).start()
    console.awaitTermination()*/

  }
}
