package polsys.sparkRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by vaijnathp on 8/6/2018.
  */
object WindowFunctions {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local").appName("Window Function").getOrCreate()
    import sparkSession.implicits._

    val empDF = sparkSession.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

    val partitionWindow = Window/*.partitionBy($"deptno")*/.orderBy($"sal".desc)
    empDF.dropDuplicates()

    val rankf = rank().over(partitionWindow)

    val rankDF=empDF.select($"*", rankf as "rank")
    rankDF/*.filter($"rank" === 1)*/.show(5)

println ("Partitions"+rankDF.rdd.getNumPartitions)

    val percentRank= rankDF.withColumn("percentRank",percent_rank().over(partitionWindow))
    percentRank.show()

    val denseRank=empDF.withColumn("DenseRank",dense_rank() over(partitionWindow))
    denseRank.show()

    val nTile=empDF.withColumn("nTileRank", ntile(4) over partitionWindow)
    nTile.show()
Thread.sleep(10001)

  }



}
