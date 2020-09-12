package polsys.sark.sql.problem

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object ProblemOnSingleTable {
  val sparkSession = SparkSession.builder.master("local").appName("Window Function").getOrCreate()
  import sparkSession.implicits._
  val workerDF = CreateTables.getWorkerDF(sparkSession).cache()

  //Write an SQL query to fetch “FIRST_NAME” from Worker table using the alias name as <WORKER_NAME>
  def selectNameAndAliasAsWorkerName():DataFrame = {
    workerDF.select("first_name").as("worker_name")
  }

  //Write an SQL query to fetch “FIRST_NAME” from Worker table in upper case.
  def selectNameAndAliasAsWorkerNameUpperCase():DataFrame = {
    workerDF.select(upper($"first_name"))
  }

  //fetches the unique values of DEPARTMENT from Worker table and prints its length.
  def selectDistinctDeptAndPrintLength():DataFrame = {
    workerDF.select("department").distinct().withColumn("deptLength",length($"department"))
  }

  // print the FIRST_NAME from Worker table after replacing ‘a’ with ‘A’.
  def replaceCharacterWithCapitalCounterPart():DataFrame = {
    workerDF.select(regexp_replace($"first_name","a","A"))
  }

  //print the FIRST_NAME and LAST_NAME from Worker table into a single column COMPLETE_NAME. A space char should separate them.
  def columnValueConcatenation():DataFrame = {
    workerDF.withColumn("complete_name",concat($"first_name",lit(" "), $"last_name")).select("complete_name")
  }

  //print all Worker details from the Worker table order by FIRST_NAME Ascending.
  def printDetailsInAscendingOrder():DataFrame = {
    workerDF.orderBy(asc("first_name"))
  }

  //print all Worker details from the Worker table order by FIRST_NAME Ascending and DEPARTMENT Descending
  def orderedByAscAndDesc():DataFrame = {
    workerDF.orderBy(asc("first_name"),desc("department"))
  }

  //print details for Workers with the first name as “Vipul” and “Satish” from Worker table.
  def filterByName():DataFrame = {
    workerDF.filter($"first_name".isin(Seq("Vipul","Satish"):_*) )
  }

  //print details of workers excluding first names, “Vipul” and “Satish” from Worker table.
  def filterByNameExcluded():DataFrame = {
    workerDF.filter(! $"first_name".isin(Seq("Vipul","Satish"):_*))
  }

  //print details of the Workers whose FIRST_NAME contains ‘a’.
  def filterIfNameHasA():DataFrame = {
    workerDF.filter($"first_name".contains("a"))
  }

  //print details of the Workers whose FIRST_NAME ends ‘a’.
  def filterNamesEndsWithA():DataFrame = {
    workerDF.filter(col("first_name").endsWith("a"))
  }

  //print details of the Workers whose FIRST_NAME ends with ‘h’ and contains six alphabets.
  def filterNamesEndsWithHandHaveSixAlphabets():DataFrame = {
    workerDF.filter(col("first_name").endsWith("h") || length($"first_name") === 6)
  }

  //print details of the Workers whose SALARY lies between 100000 and 500000.
  def printSalaryInBetween(): DataFrame = {
    workerDF.filter($"salary".between(100000,500000))
  }

  // fetch worker's complete names with salaries <= 50000 and >= 100000.
  def salaryNotInBetween(): DataFrame = {
    workerDF.filter(! $"salary".between(50000,100000))
      .select(concat($"first_name",lit(" "),$"Last_name") as("complete_name"), $"salary")
  }

  //print details of the Workers who have joined in Feb’2014
  def workerJoinedInMonth():DataFrame ={
    workerDF.filter(month($"joining_date") === 2 && year($"joining_date")===2014)
  }

  //fetch the count of employees working in the department ‘Admin’.
  def countOfAdmin():DataFrame ={
    workerDF.filter($"department" === "Admin").groupBy("department").count()
  }

  //fetch the no. of workers for each department in the descending order.
  def NumberOfWorkersPerDept(): DataFrame = {
    workerDF.groupBy("department").count().as("Count").orderBy(desc("Count"))
  }

  def main(args: Array[String]): Unit = {
    selectNameAndAliasAsWorkerName().show(5)
    selectNameAndAliasAsWorkerNameUpperCase().show(5)
    selectDistinctDeptAndPrintLength().show(5)
    replaceCharacterWithCapitalCounterPart().show(5)
    columnValueConcatenation().show(5)
    printDetailsInAscendingOrder().show(5)
    orderedByAscAndDesc().show(5)
    filterByName().show(5)
    filterByNameExcluded().show(5)
    filterIfNameHasA().show(5)
    filterNamesEndsWithA().show(5)
    filterNamesEndsWithHandHaveSixAlphabets().show(5)
    printSalaryInBetween().show(5)
    salaryNotInBetween().show(5)

    //Dates
    workerJoinedInMonth().show(5)

    //groupby
    countOfAdmin().show(5)
    NumberOfWorkersPerDept().show(5)

  }
}
