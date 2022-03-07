package polsys.spark.sql.problem

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object problem {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Creating Nested JSON object").master("local[*]").getOrCreate()
    val stru = new StructType()
    stru.add("c1",StringType)
    stru.add("c2",StringType)
    stru.add("c3",StringType)

    val txt1 = sparkSession.read.schema(stru).csv("C:\\Users\\vaijnath.polsane\\IdeaProjects\\SparkStreamingAndRDDExample\\src\\main\\resources\\Data\\txt2.txt")
//    import sparkSess ion.implicits._
//
//    val txt1Df = txt1.map(line => {
//      val l =  line.toString().replaceAll("[a-zA-z]( |$)", "").split(" ")
//      l
//    }).toDF()
//    txt1Df.show(false)

    txt1.write.mode(SaveMode.Overwrite).format("parquet").save("C:\\Users\\vaijnath.polsane\\IdeaProjects\\SparkStreamingAndRDDExample\\src\\main\\resources/Data/files")

    val df2= sparkSession.read.format("avro").load("resources/Data/")
    df2.show()

  }
}
