import org.apache.spark.{SparkConf, SparkContext}

object Boot {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf(true)
      .setMaster("local[2]")
      .setAppName("SparkAnalyzer")

    val sparkContext = new SparkContext(sparkConf)


    /**
      * Definition of accumulators for counting specific HTTP status codes
      * Accumulator variable is used because of all the updates to this variable in every executor is relayed back to the driver.
      * Otherwise they are local variable on executor and it is not relayed back to driver
      * so driver value is not changed
      */
    val httpInfo = sparkContext accumulator(0, "HTTP 1xx")


    /**
      * Iterate over access.log file and parse every line
      * for every line extract HTTP status code from it and update appropriate accumulator variable
      */
    val df = sparkContext.textFile("C:\\Users\\vaijnathp\\Desktop\\tmp")

    val mapdf = df.mapPartitions (line => {
      line.map(r => {
        httpInfo.add(1)
        r
      })
    })
    mapdf.collect()
    println("########## START ##########")
    println(s"HttpStatusInfo : ${httpInfo.value}")

    println("########## END ##########")

    sparkContext.stop()
  }

}