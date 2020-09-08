package polsys.sparkRDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vaijnathp on 3/16/2018.
  */
object sparkRDDTransOperations {

  val schema: StructType = StructType(
      List(StructField("name",StringType),StructField("class",StringType),StructField("school",StringType)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkRDD")
      .master("local[*]").config("spark.sql.shuffle.partitions","2")
      .getOrCreate()

    val dataFrame=spark.read.textFile("userLists.txt").rdd

    val letters=dataFrame.flatMap(r=> r.split(""))

    val filtered = letters.filter(l=>l.matches("[A|a|E|e|I|i|O|o]"))
    val tuplerdd=filtered.map(l=> (l,""))

    val reduced=tuplerdd.reduceByKey((acc,v)=> "" )
    reduced.foreach(e=>println(e))

  }

}
