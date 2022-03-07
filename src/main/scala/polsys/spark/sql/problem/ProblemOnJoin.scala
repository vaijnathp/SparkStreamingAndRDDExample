package polsys.spark.sql.problem

import org.apache.spark.sql.functions.{col, current_date, dense_rank, rank}
import org.apache.spark.sql.{DataFrame, SparkSession}
import polsys.sark.sql.problem.CreateTables

object ProblemOnJoin{
  val sparkSession = SparkSession.builder().appName("Join Operation").master("local[*]").getOrCreate()
  import sparkSession.implicits._
  val workerDF = CreateTables.getWorkerDF(sparkSession).persist()
  val titleDF = CreateTables.getTitleDF(sparkSession).persist()

  // print details of the Workers who are also Managers.
  def detailsOfManagers():DataFrame = {
    workerDF.as("workerDF").join(titleDF,$"worker_id" === $"worker_ref_id" && $"worker_title" === "Manager")
      .select($"workerDF.*",$"worker_title")
  }

  //fetch duplicate records having matching title and DOJ of a title table.
  def findDuplicates():DataFrame = {
    titleDF.groupBy($"worker_title", $"affected_from").count().as("count").filter($"count">1)
  }

  import org.apache.spark.sql.expressions.Window
  //show only odd rows from a table.
  def oddRowsFromTable():DataFrame = {
    val window = Window.orderBy("worker_id")
    workerDF.withColumn("rank", rank().over(window)).filter($"rank" % 2 =!= 0)
  }

  //fetch intersecting records of two tables.
  def intersectingTable(): DataFrame ={
    workerDF.intersect(workerDF.filter($"first_name" === "Nishigandha"))
  }

  //show the current date and time.
  def printCurrentTime():DataFrame = {
    workerDF.select(current_date().as("Time"))
  }

  //show the top n (say 4) records of a table.
  def top4SalariedRecords():DataFrame = {
    val colList = workerDF.columns
    val win = Window.orderBy($"salary".desc)
    workerDF.withColumn("SalaryRanking",rank().over(win)).filter($"SalaryRanking" <= 4)
  }

  //determine the nth (say n=5) highest salary from a table.
  def fifthHighestSalary():DataFrame = {
    val win = Window.orderBy($"salary".desc)
    workerDF.withColumn("rank", rank().over(win)).filter($"rank" === 5)
  }

  //fetch the list of employees with the same salary
  def workerOfSameSalary():DataFrame ={
    workerDF.groupBy($"salary").count().as("count").filter($"count" > 1).as("worker")
      .join(workerDF.as("alis"), $"alis.salary" === $"worker.salary")
      .select("alis.*")
  }

  //show one row twice in results from a table
  def unionOfDF(): DataFrame ={
    workerDF.union(workerDF)
  }

  // fetch the departments that have less than two people in it.
  def deptLessThan2Emp(): DataFrame = {
    workerDF.groupBy($"department").count().filter($"count">2)
  }

  //Print the name of employees having the highest salary in each department.
  def highestSalaryByDepartment():DataFrame = {
    val sch = workerDF.columns.map(c =>col(c))
    val window = Window.partitionBy("department").orderBy("salary")
    workerDF.withColumn("highlyPaidEmp",rank().over(window)).filter($"highlyPaidEmp"===1).select(sch:_*)
  }

  //fetch three min salaries from a table.
  def least3salaries():DataFrame = {
    val win = Window.orderBy($"salary".desc)
    workerDF.withColumn("rank",rank().over(win)).filter($"salary" <=3)
  }

  //departments along with the total salaries paid for each of them
  def deptWiseTotalSal(): DataFrame ={
    workerDF.groupBy("department").sum("salary").as("totalSalary")
  }

  def secondHighestSalaryDeptWise(): DataFrame = {
    val winOnDept = Window.orderBy("department").orderBy($"salary".desc)
    workerDF.withColumn("SalaryRank", dense_rank().over(winOnDept)).filter($"SalaryRank" === 2)
  }

  def lowestSalaryDeptWise():DataFrame = {
    val win = Window.orderBy("department").orderBy("salary")
    workerDF.withColumn("SalaryRank", dense_rank().over(win)).filter($"SalaryRank" === 1)
  }


  def main(args: Array[String]): Unit = {
    print("")
//    detailsOfManagers().show(5)
//    findDuplicates().show(5)
//    oddRowsFromTable().show(5)
//    intersectingTable().show(5)
//    printCurrentTime().show(5)
//    top4SalariedRecords().show(5)
//    fifthHighestSalary().show(5)
//    workerOfSameSalary().show(5)
//    unionOfDF().show(5)
//    deptLessThan2Emp().show(5)
//    highestSalaryByDepartment().show(5)
//    deptWiseTotalSal().show(5)
    secondHighestSalaryDeptWise().show()
    lowestSalaryDeptWise().show()


  }


}
