package polsys.structuredStreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by vaijnathp on 5/30/2018.
  */
object AggregateExample {
  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    import sparkSession.implicits._

    val csvDF=sparkSession.readStream
      .schema(StructType(List(StructField("Name",StringType),StructField("Address",StringType),StructField("timestamp",TimestampType))))
      .option("delimiter",",")
      /*.format("csv")*/.csv("tmp")

//    val withTimeDF=csvDF.withColumn("timestamp1",current_timestamp())

//    val convDF=csvDF.as[(Stirng,String,TimeStamp)]
    val countDF:DataFrame=csvDF.withWatermark("timestamp", "5 seconds")
      .groupBy(window($"timestamp","10 seconds"),$"name").count()/*.sort($"window")*/

//    val IronMan= countDF.groupBy(window($"window.start","20 seconds"))
val console=csvDF.withWatermark("timestamp", "5 seconds")
  .groupBy(window($"timestamp","10 seconds"),$"name").count().writeStream.format("console")
  .option("truncate",value = false).outputMode("update").start()
    console.awaitTermination()

    /*val console= countDF.writeStream.format("console")
      .option("checkpointLocation", "vaij")
      .option("path","vaijnath")
      .outputMode(OutputMode.Append()).start()
    console.awaitTermination()
*/
  }

}
