package polsys.spark.sql.problem

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object NetstedJSONToCSV {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Creating Nested JSON object").master("local[*]").getOrCreate()

    import sparkSession.implicits._
    val json: DataFrame = sparkSession.read.option("multiLine", true).option("inferSchema", "true").json("C:\\Users\\vaijnath.polsane\\Downloads\\ChecklistQuestions.json")

//    val j1=json.select(explode(col("Instances")))
//    j1.printSchema()
    val j1 = json.select(explode(col("Instances.element")))
    j1.printSchema()
    j1.show(truncate = false)

    json.write.mode(SaveMode.Overwrite).option("header",true).csv("src/main/resources/Data/output")
  }
}
