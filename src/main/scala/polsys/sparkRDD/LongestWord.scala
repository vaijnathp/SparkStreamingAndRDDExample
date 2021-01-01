package polsys.sparkRDD

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by vaijnathp on 8/6/2018.
  */
object LongestWord {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local[*]").appName("LongestWord").getOrCreate()
    import session.implicits._
    val df = session.read.textFile("Names.txt")

//    val wordLen=df.map(word => Row.fromSeq(Seq(word.length,word)))(RowEncoder(StructType(List(StructField("Len",IntegerType),StructField("Word",StringType)))))

    val wordLen= df.map(r=> Row(r))(RowEncoder(df.schema))
      .map(words=> (words.length,words)).toDF("Len","Word")
    val winFun=Window.orderBy($"Len".desc)

    val rankv = rank().over(winFun)

    wordLen.select($"*", rankv as "Rank").filter($"Rank" === 1).show()



   /* val sorted=wordLen.sortBy(t => t._1, ascending = false)

    sorted.collect().take(1).foreach(println)*/


  }
}
