package polsys.structuredStreaming


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vaijnathp on 5/30/2018.
    */
    object WindowingWordCount {
      def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    import sparkSession.implicits._
    import org.apache.spark.sql.catalyst.expressions._

    val csvDF=sparkSession.readStream
      .schema(StructType(List(StructField("Name",StringType),StructField("Address",StringType))))
      .option("delimiter",",")
      /*.format("csv")*/.csv("C:\\Users\\vaijnathp\\Desktop\\tmp")

    val withTimeDF=csvDF.withColumn("timestamp",current_timestamp())

    val countDF=withTimeDF.groupBy(window($"timestamp", "10 seconds", "5 seconds"),$"Name").agg(collect_list("name"))

    val console=countDF.writeStream.format("console").outputMode(OutputMode.Complete()).start()

    console.awaitTermination()

  }

}
