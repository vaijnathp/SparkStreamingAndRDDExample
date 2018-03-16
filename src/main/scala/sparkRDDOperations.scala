import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vaijnathp on 3/16/2018.
  */
object sparkRDDOperations {

  val schema: StructType = StructType(
      List(StructField("name",StringType),StructField("class",StringType),StructField("school",StringType)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkRDD")
      .master("local[*]").config("spark.sql.shuffle.partitions","2")
      .getOrCreate()

val dataFrame=spark.read.schema(schema).csv("C:\\Users\\vaijnathp\\Desktop\\T.csv")

    val filteredDF=dataFrame.filter(row=> true)

    filteredDF.show()

    dataFrame.show()

  }

}
