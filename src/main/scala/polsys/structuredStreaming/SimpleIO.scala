package polsys.structuredStreaming

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SimpleIO {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("SimpleIO").getOrCreate()

    val inputDF = sparkSession.readStream.schema(StructType(List(StructField("Text", StringType)))).option("delimiter", "$").csv("input")

    println("Output start at: "+new Date())
    inputDF.writeStream.queryName("outputStream").format("csv").option("path", "vaij/out").option("checkpointLocation", "vaij/checkpoint").outputMode(OutputMode.Append()).start()
  println("Job finished at: "+new Date())
    sparkSession.streams.awaitAnyTermination()
  }
}