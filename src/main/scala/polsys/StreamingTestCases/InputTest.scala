package polsys.StreamingTestCases

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import collection.JavaConverters._
/**
  * Created by vaijnathp on 8/17/2018.
  */
object InputTest {

  def getData(sparkSession: SparkSession,stream: DataFrame):List[Row] = {

    stream.writeStream.format("memory").queryName("outputQuery")
      .outputMode(OutputMode.Append())
      .start()
      .processAllAvailable()

    sparkSession.sqlContext.sql("select * from outputQuery")
      .collectAsList()
      .asScala.toList
  }


  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().appName("TestCases").master("local[*]").getOrCreate()

    val vaij=sparkSession.readStream.schema(StructType(List(StructField("name",StringType)))).csv("input")

    val list=getData(sparkSession,vaij)

    list.foreach(println)

  }

}