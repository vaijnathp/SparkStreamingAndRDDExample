package polsys.structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
  * Created by vaijnathp on 5/30/2018.
  */
object TransWithWindow {
  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
import sparkSession.implicits._
    val csvDF=sparkSession.readStream
      .schema(StructType(List(StructField("Name",StringType),StructField("id",IntegerType),StructField("sal",DoubleType),StructField("Manager",StringType),StructField("age",IntegerType),StructField("savings",DoubleType),StructField("DOJ",TimestampType))))
      .option("delimiter","|")
      .csv("C:\\Users\\vaijnathp\\IdeaProjects\\Hydrograph_Streaming\\hydrograph.engine\\TestJobs\\input")
    val transWindow=csvDF.select('name,'id, 'sal + 100 as 'UpdatedSal, 'Manager, 'age, 'savings, 'DOJ)


    transWindow.writeStream.queryName("aggregates").format("csv").option("delimiter",",")
      .option("checkpointLocation", "C:\\Users\\vaijnathp\\IdeaProjects\\SparkStreamingAndRDDExample\\vaijnathCheckpoint")
      .outputMode(OutputMode.Append())
      .option("path","C:\\Users\\vaijnathp\\IdeaProjects\\SparkStreamingAndRDDExample\\vaijnath")
      .start()















  }
}
