package polsys.structuredStreaming

import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, CreateNamedStructLike, Expression, Generator, NamedExpression}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vaijnathp on 5/30/2018.
  */
object WindowingWithFileOutputWordCount {
  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder().master("local[*]").appName("Structured Stream").getOrCreate()
    import sparkSession.implicits._

    val csvDF=sparkSession.readStream
      .schema(StructType(List(StructField("Name",StringType),StructField("Address",StringType))))
      .option("delimiter",",")
      /*.format("csv")*/.csv("C:\\Users\\vaijnathp\\IdeaProjects\\Hydrograph_Streaming\\hydrograph.engine\\TestJobs\\input")

    val withTimeDF=csvDF.withColumn("timestamp",current_timestamp())

    //val win= withTimeDF.withColumn("window",window($"timestamp", "10 seconds", "5 seconds"))
    //val newdf=withTimeDF.withWatermark("timestamp","10 seconds").withColumn("window",window($"timestamp", "10 seconds", "10 seconds"))

    //val resultDF= newdf.groupBy($"window"/*window($"timestamp", "10 seconds", "5 seconds")*/,$"Name").count()

    val win=window($"timestamp", "10 seconds", "5 seconds").as("window")
    val countDF=withTimeDF./*withColumn("window",window($"timestamp", "10 seconds", "10 seconds")).*/withWatermark("timestamp","10 seconds")
    val p=countDF.groupBy(win.asInstanceOf[Column].as("v"),$"Name").count()



//    println(win.toString().contains("window"))
println(named(win.expr).name.equals("window"))
    p.explain(false)

   // resultDF.writeStream.format("console").outputMode(OutputMode.Complete()).start().awaitTermination()

   /*val query= countDF.select($"name",$"count",$"window.start",$"window.end").writeStream.queryName("aggregates").format("csv").option("delimiter",",")
      .option("checkpointLocation", "C:\\Users\\vaijnathp\\IdeaProjects\\SparkStreamingAndRDDExample\\vaijnathCheckpoint")
        .outputMode(OutputMode.Append())
      .option("path","C:\\Users\\vaijnathp\\IdeaProjects\\SparkStreamingAndRDDExample\\vaijnath")
      .start()*/

    val query=p.writeStream.queryName("aggregates").outputMode("complete")
      .format("memory")
      .start()

    sparkSession.sql("select * from aggregates").show()

    //query.awaitTermination()

  }

  def named(expr : Expression): NamedExpression = expr match {
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
  }
}
