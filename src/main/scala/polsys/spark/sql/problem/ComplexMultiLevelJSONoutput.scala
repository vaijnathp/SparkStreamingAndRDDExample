package polsys.spark.sql.problem

import org.apache.spark.sql.functions.{col, collect_list, lit, struct}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object ComplexMultiLevelJSONoutput {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Creating Nested JSON object").master("local[*]").getOrCreate()

    import sparkSession.implicits._
    val csvFile: DataFrame = sparkSession.read.option("inferSchema" , "true").option("header", "true").option("delimiter", "|").csv("src/main/resources/Data/SHE_indicators_20201228.csv")

val groColSeq = Seq(col("Indicatorlabel"),col("Indicatorcategory"),col("ThirdPartyAssured"),
  col("UniversalIndicator"),col("UnitOfMeasure"),col("GroupLevelDisclosure"),
  col("TypeOfDisclosure"),col("NameOfDisclosure"))

    val df = csvFile.groupBy(col("Indicatorlabel"),col("Indicatorcategory"),col("ThirdPartyAssured"),
      col("UniversalIndicator"),col("UnitOfMeasure"),col("GroupLevelDisclosure"),
      col("TypeOfDisclosure"),col("NameOfDisclosure"))
      .agg(collect_list(struct(col("ReadingDate").as("submissionDate"),
      col("IndicatorValue"), col("IndicatorComment").as("comment"))).as("Readings"))
      .groupBy(col("Indicatorlabel"),col("Indicatorcategory"),col("ThirdPartyAssured"),
        col("UniversalIndicator"),col("UnitOfMeasure"),col("GroupLevelDisclosure"),
        col("TypeOfDisclosure"),col("NameOfDisclosure"))
      .agg(collect_list(struct(col("NameOfDisclosure").as("AssetName"),col("Readings"))).as("GrpTypeOfDisclosure"))
      .groupBy(col("Indicatorlabel"),col("Indicatorcategory"),col("ThirdPartyAssured"),
        col("UniversalIndicator"),col("UnitOfMeasure"))
      .agg(collect_list(functions.map(col("TypeOfDisclosure"),col("GrpTypeOfDisclosure"))).as("IndicatorDetails"))

    df.write.mode(SaveMode.Overwrite).json("src/main/resources/Data/output")
  }

}
