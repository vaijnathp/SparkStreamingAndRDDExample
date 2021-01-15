package com.polsys

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object AppDataOperations {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[*]").appName("LongestWord").getOrCreate()

    val schema = new StructType().add("app_id", StringType)
      .add("app_store", StringType)
      .add("app_url", StringType)
      .add("version", StringType)
      .add("supported_language",ArrayType(StringType))
      .add("category", StringType)
    val appData = session.read.schema(schema).option("inferSchema",true).json(args(0)).cache()
    val appAvailability = appData.filter(col("app_store").isin(Seq("itunes","playstore"):_*) || col("app_url").isNotNull)
    println("************valid app based on availability on itunes or playstore or valid app store URL*********")
    appAvailability.show(truncate = false)

    val appFromItunes = appData.filter(col("app_store")==="itunes")
    println("************valid app based only from itunes*********")
    appFromItunes.show(truncate = false)

    val gamingApp = appData.filter(col("app_store")==="itunes" and col("category")==="Gaming")
    println("************valid app based only from itunes which are of Gaming category*********")
    gamingApp.show(truncate = false)

  }
}
