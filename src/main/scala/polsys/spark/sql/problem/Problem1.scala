package polsys.sark.sql.problem

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object Problem1 {
  val sparkSession = SparkSession.builder.master("local").appName("Window Function").getOrCreate()
  import sparkSession.implicits._

  //Write an SQL query to fetch “FIRST_NAME” from Worker table using the alias name as <WORKER_NAME>
  def selectNameAndAliasAsWorkerName():DataFrame = {
    val workerDF = CreateTables.getWorkerDF(sparkSession)
    workerDF.select("first_name").as("worker_name")
  }

  //Write an SQL query to fetch “FIRST_NAME” from Worker table in upper case.
  def selectNameAndAliasAsWorkerNameUpperCase():DataFrame = {
    val workerDF = CreateTables.getWorkerDF(sparkSession)
    workerDF.select(upper($"first_name"))
  }

  def main(args: Array[String]): Unit = {
    selectNameAndAliasAsWorkerName().show(5)
    selectNameAndAliasAsWorkerNameUpperCase().show(5)
  }

}
