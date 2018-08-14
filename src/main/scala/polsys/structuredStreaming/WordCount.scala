package polsys.structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
/**
  * Created by vaijnathp on 5/30/2018.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    import sparkSession.implicits._

    val lines=sparkSession.readStream.format("socket").option("host","bhsen").option("port","9999").load()
    val words=lines.as[String].flatMap(_.split(" "))

    val wordCounts=words.groupBy("value").count()

    val console=wordCounts.writeStream.format("console").outputMode(OutputMode.Complete()).start()

    console.awaitTermination()

  }

}
