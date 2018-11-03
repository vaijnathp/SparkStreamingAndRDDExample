package polsys.sparkRDD

import org.apache.spark.sql.SaveMode

import scala.io.Source
import org.apache.spark.sql.functions._


/**
  * Created by vaijnathp on 3/26/2018.
  */

object glue {

 /* def main(args: Array[String]): Unit = {
    glueCall("C:\\Users\\vaijnathp\\Desktop\\T.csv","asdf")

  }*/

def glueCall(src: String , tgt: String): Unit ={
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;


val broad = spark.sparkContext.broadcast(col("vaij"))
}

}
