package polsys.spark.sql.problem

import org.apache.spark.sql.functions.{col, collect_list, first, lit, struct}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object CreateNestedJSONoutput {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Creating Nested JSON object").master("local[*]").getOrCreate()

    val empCSVschema: StructType = StructType(List(StructField("id", IntegerType),
      StructField("name", StringType), StructField("designation", StringType),
      StructField("managerid", IntegerType), StructField("DOJ", DateType),
      StructField("salary", IntegerType), StructField("deptid", IntegerType)))

    import sparkSession.implicits._
    val csvFile: DataFrame = sparkSession.read.schema(empCSVschema).option("delimiter", "|").csv("src/main/resources/Data/employee.csv")

    val jsonDF = csvFile.groupBy("deptid", "managerid")
      .agg(collect_list(functions.map(lit("id"),$"id",lit("name"),$"name",lit("salary"),$"salary")).as("map")).toJSON

    jsonDF.show(truncate = false)

    jsonDF.write.mode(SaveMode.Overwrite).json("C:\\Users\\vaijnath.polsane\\IdeaProjects\\SparkStreamingAndRDDExample\\src\\main\\resources\\Data\\output")

  }
}