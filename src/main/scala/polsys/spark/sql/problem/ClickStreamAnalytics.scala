package polsys.spark.sql.problem

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ClickStreamAnalytics {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ClickStreamAnalytics").master("local[*]").getOrCreate()
    val schema =StructType(Seq(StructField("event_time",TimestampType,true),StructField("user",StringType,true)))
    val data = sparkSession.read.schema(schema).csv("src/main/resources/Data/clickStreamData.txt")

    val df = data.withColumnRenamed("_c0","event_time").withColumnRenamed("_c1","user")
      .withColumn("last_accessed",lag("event_time",1)
        .over(Window.partitionBy("user").orderBy("event_time")))
      .withColumn("inactive_time", (col("event_time").cast(LongType) - col("last_accessed").cast(LongType))/60)
      .withColumn("session_start",when(col("inactive_time").isNull,
        first("event_time").over(Window.partitionBy("user").orderBy("event_time")))
        .otherwise(col("event_time")))
      .withColumn("session_id_", when(session_condition,sessionId))
      .withColumn("grp",count("session_id_").over(Window.orderBy("event_time")))
      .withColumn("session_id", first("session_id_").over(Window.partitionBy("grp").orderBy("event_time")) )
      .select("event_time","user","session_id")

    df.write.mode(saveMode = SaveMode.Overwrite ).parquet("src/main/resources/Data/output_click_stream")

//============================================Second Problem=======================================================

    val sessionClickStreamData = sparkSession.read.parquet("src/main/resources/Data/output_click_stream")

    val resultDF = sessionClickStreamData.withColumn("event_date",to_date(col("event_time")))

    val numberOfSessionPerDay = resultDF.groupBy(col("event_date")).agg(countDistinct("session_id")).as("number_of_session_per_day")
    val totalTimeSpentByUserByPerDay = resultDF.groupBy(col("event_date"),col("user"))
      .agg((last(col("event_time").cast(LongType))-first(col("event_time").cast(LongType)))/3600).as("total_hours_spent_by_user_by_per_day")

    numberOfSessionPerDay.show(100,truncate = false)
    totalTimeSpentByUserByPerDay.show(100,truncate = false)

  }

  private def session_condition = {
    (col("event_time").cast(LongType) - col("session_start").cast(LongType)) > 7200 || col("inactive_time") > 30 || col("last_accessed").isNull
  }

  def sessionId: Column={
    concat(col("user"),lit("_"),row_number().over(Window.partitionBy("user").orderBy("event_time")))
  }
}
