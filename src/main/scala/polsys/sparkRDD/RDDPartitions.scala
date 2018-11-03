package polsys.sparkRDD

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by vaijnathp on 9/10/2018.
  */
object RDDPartitions {

  def main(args: Array[String]): Unit = {

    val ss=SparkSession.builder().appName("MyApp").master("local[1]").getOrCreate()

    val df = ss.read.textFile("input")

    println("****************"+df.rdd.getNumPartitions+"******************")

    df.write.mode(SaveMode.Overwrite).text("out")
    df.show()
    println("Job finished")

  }

}
