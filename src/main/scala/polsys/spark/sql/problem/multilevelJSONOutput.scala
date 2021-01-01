package polsys.spark.sql.problem

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object multilevelJSONOutput {
  /*[{
    "vendor_name": "Vendor 1",
    "count": 10,
    "categories": [{
    "name": "Category 1",
    "count": 4,
    "subCategories": [{
    "name": "Sub Category 1",
    "count": 1
  },
  {
    "name": "Sub Category 2",
    "count": 1
  },
  {
    "name": "Sub Category 3",
    "count": 1
  },
  {
    "name": "Sub Category 4",
    "count": 1
  }
    ]
  }]*/

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Creating Nested JSON object").master("local[*]").getOrCreate()


    import sparkSession.implicits._
    val csvFile: DataFrame = sparkSession.read.option("inferSchema" , "true").option("header", "true").option("delimiter", "|").csv("src/main/resources/Data/nestedJsonInput.csv")
    val jsonDF = csvFile.groupBy("Vendor_Name","count","Categories","Category_Count")
      .agg(collect_list(struct(col("Subcategory"),
        col("Subcategory_Count"))).alias("subCategories"))/*.toJSON*/
      .groupBy("Vendor_Name","count")
      .agg(collect_list(struct(col("Categories")
        ,col("Category_Count"),col("subCategories"))).alias("categories")).toJSON



    jsonDF.show(truncate = false)
//    jsonDF.write.mode(SaveMode.Overwrite).json("C:\\Users\\vaijnath.polsane\\IdeaProjects\\SparkStreamingAndRDDExample\\src\\main\\resources\\Data\\output")
  }
}
