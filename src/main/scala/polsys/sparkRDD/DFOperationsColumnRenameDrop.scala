package polsys.sparkRDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * Created by vaijnathp on 8/13/2018.
  */
object DFOperationsColumnRenameDrop {

  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("columnOperations").master("local[*]").getOrCreate()

val stru=new StructType()
    stru.add("fname",StringType)
    stru.add("Lname",StringType)

    val df=sparkSession.read.
      schema(StructType(List(StructField("Name",StructType(List(StructField("Fname",StringType),StructField("Lname",StringType)))),
      StructField("Address",new StructType().add("LaneNo",StringType).add("LandMark",StringType))))).option("delimeter",",")
      .json("input")
    import sparkSession.implicits._
    df.printSchema()
    val renamedDf=df.withColumnRenamed($"Name.Fname".toString(),"FName")
    renamedDf.show()
    val copiedDF=renamedDf.withColumn("Lname",col("FName"))
    copiedDF.show()
    val copiedWithDotDF=renamedDf.withColumn("Lname.abc",col("FName"))
    copiedWithDotDF.show()
    val rename=copiedWithDotDF.withColumnRenamed("Lname.abc","abc")
    rename.show()
    val dropedDF=copiedDF.drop($"Lname")
    dropedDF.show()

  }


}
