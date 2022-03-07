package polsys.spark.sql

import org.apache.spark.sql.SparkSession

package object SqlProblems {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sqlProblems").getOrCreate()


  }
}
