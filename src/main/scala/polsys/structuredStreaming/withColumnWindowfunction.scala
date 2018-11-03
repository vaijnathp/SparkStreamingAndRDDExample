package polsys.structuredStreaming

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
  * Created by vaijnathp on 5/30/2018.
  */
object withColumnWindowfunction {
  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    import sparkSession.implicits._

    val csvDF=sparkSession.readStream
      .schema(StructType(List(StructField("Name",StringType),StructField("id",IntegerType),StructField("sal",DoubleType),StructField("Manager",StringType),StructField("age",IntegerType),StructField("savings",DoubleType),StructField("DOJ",TimestampType))))
      .option("delimiter","|")
      .csv("C:\\Users\\vaijnathp\\IdeaProjects\\Hydrograph_Streaming\\hydrograph.engine\\TestJobs\\input")

    val withTimeDF=csvDF.withColumn("timestamp",current_timestamp())


    val countDF=withTimeDF/*.withWatermark("timestamp","10 seconds")*/
    var winDF= countDF.withColumn("window",window($"timestamp", "10 seconds", "5 seconds"))
    winDF= winDF.withWatermark("timestamp","10 seconds")
    val p=winDF.groupBy($"Name").count()

    p.explain(true)

    p.writeStream.queryName("aggregates").format("csv").option("delimiter",",")
      .option("checkpointLocation", "C:\\Users\\vaijnathp\\IdeaProjects\\SparkStreamingAndRDDExample\\vaijnathCheckpoint")
      .outputMode(OutputMode.Append())
      .option("path","C:\\Users\\vaijnathp\\IdeaProjects\\SparkStreamingAndRDDExample\\vaijnath")
      .start()

//    sparkSession.sql("select * from aggregates").show()


  }

 /* def named(expr : Expression): NamedExpression = expr match {
    // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
    // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
    // make it a NamedExpression.
    case u: UnresolvedAttribute => UnresolvedAlias(u)

    case u: UnresolvedExtractValue => UnresolvedAlias(u)

    case expr: NamedExpression => expr

    // Leave an unaliased generator with an empty list of names since the analyzer will generate
    // the correct defaults after the nested expression's type has been resolved.
    case g: Generator => MultiAlias(g, Nil)

    case func: UnresolvedFunction => UnresolvedAlias(func, Some(generateAlias))

    // If we have a top level Cast, there is a chance to give it a better alias, if there is a
    // NamedExpression under this Cast.
    case c: Cast =>
      c.transformUp {
        case c @ Cast(_: NamedExpression, _, _) => UnresolvedAlias(c)
      } match {
        case ne: NamedExpression => ne
        case _ => Alias(expr, toPrettySQL(expr))()
      }

    case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
      UnresolvedAlias(a, Some(generateAlias))

    // Wait until the struct is resolved. This will generate a nicer looking alias.
    case struct: CreateNamedStructLike => UnresolvedAlias(struct)

    case expr: Expression => Alias(expr, toPrettySQL(expr))()
  }
  def generateAlias(e: Expression): String = {
    e match {
      case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
        a.aggregateFunction.toString
      case expr => toPrettySQL(expr)
    }
  }*/
}
