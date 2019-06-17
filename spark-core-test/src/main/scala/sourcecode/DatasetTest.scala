package sourcecode

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoders, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object DatasetTest {
  case class People(name: String, age: Int, job: String)
  case class PeopleWithoutJob(name: String, age: Int)
  case class Address(name: String, address: String)

  def dtypesTest(spark: SparkSession, peopleDS: Dataset[People]): Unit = {
    peopleDS.dtypes.foreach(a => println(a._1 + ":" + a._2))

    println(s"is local dataset: ${peopleDS.isLocal}")
  }

  def sortWithinPartitionsTest(spark: SparkSession, peopleDS: Dataset[People]): Unit = {
    println(s"the number of partitions about peopleDS is:${peopleDS.rdd.getNumPartitions}")
    peopleDS.repartition(1).sortWithinPartitions("age").show
  }

  def sortTest(spark: SparkSession, peopleDS: Dataset[People]): Unit = {
    peopleDS.sort(col("age")).show()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("dataset test")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    val peopleDS = spark
      .sparkContext
      .textFile("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.csv")
      .map(peopleStr => peopleStr.split(";"))
      .filter(arr => !arr(1).equalsIgnoreCase("age"))
      .map(peopleArr => People(peopleArr(0), peopleArr(1).trim.toInt, peopleArr(2)))
      .toDS

    peopleDS.printSchema()


    // 1st solution
    val maxNum = 30
    peopleDS.where("num = 30").show()
    // 2st solution
    val windowSpec = Window.orderBy(col("age").desc)
    peopleDS.withColumn("rankNo", rank().over(windowSpec)).where("rankNo = 1").show()

    // 3st solution (no sorting)
//    peopleDS.rdd.mapPartitions(iter => iter.)


    return
    //peopleDS.show()
    // toString
    // println("to string of dataset:" + peopleDS)

    // dtypes
    // dtypesTest(spark, peopleDS)

    // sortWithinPartitions
    // sortWithinPartitionsTest(spark, peopleDS)

    // sort
    // sortTest(spark, peopleDS)

    // select
    // peopleDS.select(col("age").alias("Age"), abs(col("age")).as("absAge"), col("*")).show()

    // groupBy
    // peopleDS.groupBy("name", "job").agg(avg(col("age"))).show()

    // reduce
    // println(peopleDS.reduce((p1, p2) => if (p1.age > p2.age) p1 else p2))

    // rollup
    // peopleDS.rollup("name", "job").avg("age").show()

    // cube
    // peopleDS.cube("name", "job").avg("age").show()

    // dropDuplicates
    // peopleDS.dropDuplicates("name", "age").show

    // filter
    // peopleDS.filter("age < 100").show()

    // map
    peopleDS.map(people => PeopleWithoutJob(people.name, people.age)).show

    // agg
    peopleDS.agg("age" -> "avg").show
    peopleDS.groupBy("name").agg(max(col("age")).as("maxAge"), avg(col("age")).as("avgAge"), first(col("age"))).show
    peopleDS.groupBy("name").agg("age" -> "max", "age" -> "avg", "age" -> "first").show

    // window spec
    val ws = Window.partitionBy("name").orderBy(col("age").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
//    peopleDS
//      .withColumn("rankId", rank().over(ws))
//      .withColumn("rowNumber", row_number().over(ws))
//      .filter("rankId = 1")
//      .drop("rankId", "rowNumber")
//      .show
    peopleDS
      .withColumn("sum", sum("age").over(ws))
      .withColumn("avg", avg("age").over(ws))
      .show

    // complex column
    peopleDS.selectExpr("struct(name, age) as pair").show()
    val complexDS = peopleDS.withColumn("pair", struct("name", "age"))
    complexDS.show()

    // udf practice
    def addOne(age: Int): Int = age + 100
    val addOneUDF = udf(addOne(_:Int):Int)
    val add1 = spark.udf.register("addOne", addOneUDF)
    val add2 = spark.udf.register("addOne", addOne(_:Int):Int)
    peopleDS.selectExpr("age", "addOne(age)").show
    peopleDS.select(col("age"), add1(col("age")).as("udf_fun")).show
    def addNameAge(name: String, age: Int) = name + "_" + age
    val combineCol = spark.udf.register("combineCol", addNameAge(_:String, _:Int):String)
    peopleDS.select(col("name"), col("age"),  combineCol(col("name"), col("age")).as("combine_col")).show
    val testUDF = spark.udf.register("test", (s: Int) => s + 1)
    peopleDS.select(col("name"), col("age"), testUDF(col("age"))).show()
    return

    // join
    val addressSchema = Encoders.product[Address].schema
    val addressDF = spark.read
      .option("inferSchema", "true")
      .option("header", "false")
      .option("sep", ";")
      .schema(addressSchema)
      .csv("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/address.txt")
      .as[Address]

    addressDF.show()
    addressDF.printSchema()
    peopleDS.join(addressDF, peopleDS.col("name")===addressDF.col("name"), "inner").show
    peopleDS.join(broadcast(addressDF), peopleDS.col("name") === addressDF.col("name")).explain()
  }

}
