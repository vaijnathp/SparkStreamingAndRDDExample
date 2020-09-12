package polsys.sark.sql.problem

//https://www.techbeamers.com/sql-query-questions-answers-for-practice/

import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateTables {
  private val worker = Seq((1,"Manjusha","Sane",100000,"2014-02-20 09:00:00","HR"),
    (2,"Nishigandha","Pawar",80000,"2014-06-11 09:00:00","Admin"),
    (3,"Vishal","Shinde",300000,"2014-02-20 09:00:00","HR"),
    (4,"Akshay","Patil",500000,"2014-02-20 09:00:00","Admin"),
    (8,"Aarjun","Patil",500000,"2014-02-20 09:00:00","HR"),
    (5,"Vivek","Kulkarni",500000,"2014-06-11 09:00:00","Admin"),
    (6,"Vikas","Kolhe",200000,"2014-06-11 09:00:00","Account"),
    (7,"Satish","Karad",75000,"2014-01-20 09:00:00","Account"))
  private val workerSchema = Seq("worker_id","first_name","last_name","salary","joining_date","department")

  private val bonus = Seq(("1","2016-02-20 00:00:00","5000"),
    ("2","2016-06-11 00:00:00","3000"),
    ("3","2016-02-20 00:00:00","4000"),
    ("1","2016-02-20 00:00:00","4500"),
    ("2","2016-06-11 00:00:00","3500"))
  private val bonusSchema = Seq("worker_ref_id","bonus_date","bonus_amount")

  private val title = Seq(("1","Manager","2016-02-20 00:00:00"),
    ("2","Executive","2016-06-11 00:00:00"),
    ("8","Executive","2016-06-11 00:00:00"),
    ("5","Manager","2016-06-11 00:00:00"),
    ("4","Asst. Manager","2016-06-11 00:00:00"),
    ("7","Executive","2016-06-11 00:00:00"),
    ("6","Lead","2016-06-11 00:00:00"),
    ("3","Lead","2016-06-11 00:00:00"))
  private val titleSchema = Seq("worker_ref_id","worker_title","affected_from")

  def getWorkerDF(sparkSession: SparkSession):DataFrame = {
    sparkSession.createDataFrame(worker).toDF(workerSchema:_*)
  }
  def getBonusDF(sparkSession: SparkSession):DataFrame = {
    sparkSession.createDataFrame(bonus).toDF(bonusSchema:_*)
  }
  def getTitleDF(sparkSession: SparkSession):DataFrame = {
    sparkSession.createDataFrame(title).toDF(titleSchema:_*)
  }
}
