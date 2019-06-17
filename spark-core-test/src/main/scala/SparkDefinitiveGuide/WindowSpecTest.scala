package SparkDefinitiveGuide

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowSpecTest {

  case class Animal(name: String, size: Int, age: Int, alias: String)

  val filePath = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/animal.txt"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("window spec test")
      .master("local[2]")
      .getOrCreate()

    val animalRdd = spark.read.textFile(filePath)
    val endocer = Encoders.product[Animal]
    import spark.implicits._
    spark.read.schema(endocer.schema).option("header", false).option("sep", ",").csv(filePath).as[Animal].printSchema()

    animalRdd.collect().foreach(println)

    import spark.implicits._
    val animalDS = animalRdd.map(animalStr => {
      val animalArr = animalStr.split(",")
      Animal(animalArr(0), animalArr(1).trim.toInt, animalArr(2).trim.toInt, animalArr(3))
    }).as[Animal]

    animalDS.printSchema()

    val ws = Window.partitionBy("name").orderBy(col("age").desc)
    val rankNumber = rank().over(ws)
    val rowNumber = row_number().over(ws)
//    animalDS.withColumn("rowNumber", rowNumber).withColumn("rankNumber", rankNumber).show()
    animalDS
      .withColumn("rowNumber", rowNumber)
      .withColumn("rankNumber", rankNumber)
      .where("rowNumber = 1")
      .drop("rowNumber", "rankNumber")

    animalDS.show()
    animalDS
      .withColumn("sumAge", sum("age").over(ws))
      .select("name", "age", "sumAge")
      .show

    animalDS.select($"name").show
  }

}
