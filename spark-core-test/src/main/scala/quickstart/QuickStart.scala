package quickstart

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by jiezhou on 22/11/2018.
 */
object QuickStart {

  case class Staff(name: String, age: Int, role: String)



  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("quick start")
      .master("local[2]")
      .getOrCreate()

    val peopleDS = spark.read.textFile("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt")
    peopleDS.show(10)

    import spark.implicits._
    val staffDS = peopleDS.map(_.split(",")).map(row => Staff(row(0), row(1).trim.toInt, row(2)))
    staffDS.printSchema()
    staffDS.show()

    // find the line with the most words in pom.txt
    val pomDS = spark.read.textFile("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/pom.txt")
    pomDS.show()
    /*find the the largest size*/
    println(pomDS.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))

    val countSizeUDF = spark.udf.register("countSize", countSize(_:String):Int)
    println(pomDS.withColumn("count", countSizeUDF(col("value"))).orderBy(col("count").desc).head())

    // add row number for all the rows
    pomDS.withColumn("rowNumber", monotonically_increasing_id()).show()

    // find the line with the most words
    val rowWithMostWords = pomDS.withColumn("count", udf((s: String) => s.split(" ").size).apply(col("value")))
      .reduce((a, b) => if (a.getInt(1) > b.getInt(1)) a else b)
    println(s"The row with most words is :${rowWithMostWords.get(0)}, and the count of words is : ${rowWithMostWords.getInt(1)}")

  }

  def countSize(col: String): Int = {
    col.split(" ").size
  }
}
