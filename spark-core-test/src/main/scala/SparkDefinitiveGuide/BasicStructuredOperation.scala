package SparkDefinitiveGuide

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object BasicStructuredOperation {

  private val JSON_FILE_PATH = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.json"

  def sqlTest(spark: SparkSession, dataframe: DataFrame): Unit = {
    dataframe.select(
      dataframe.col("name"),
      col("name"),
      column("name"),
      expr("name"))
      .show(2)


    dataframe.select(expr("name as new_name")).show(3)


    // select expr
    dataframe.selectExpr( "avg(age)").show

    // add constant as new column
    dataframe.select(expr("*"), lit(1).alias("one")).show

    // add new column
    dataframe.withColumn("constant_column", lit(100)).show
    // rename existing column
    dataframe.withColumnRenamed("name", "rename").show


  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("test")
      .master("local[3]")
      .getOrCreate()

    // read json
    val jsonDataset = spark.read.format("json").load(JSON_FILE_PATH)

    jsonDataset.show()
    jsonDataset.printSchema()

    // How to refer to column
    jsonDataset.select(col("name")).show()
    jsonDataset.select(column("name")).show()
    jsonDataset.select(jsonDataset.col("name")).show()
    jsonDataset.select(expr("name")).show()
    jsonDataset.select(expr("age + 100")).show()
    jsonDataset.columns.foreach(println)

    // Create View
    jsonDataset.createOrReplaceTempView("dfTable")
    spark.sql("select name, age from dfTable").show()

    // Schema and Row
    val rowSeq = Seq(
      Row("zhoujielun", 100, "taiwan"),
      Row("zhangsanfeng", 200, "changan"),
      Row("zhangxueliang", 150, "dongbei"))

    val schema = StructType(List(
      StructField("name", StringType, false),
      StructField("age", IntegerType, false),
      StructField("address", StringType, false)
    ))

    val rowRdd = spark.sparkContext.parallelize(rowSeq)
    val dataframe = spark.createDataFrame(rowRdd, schema)

    dataframe.show()

    // sql
    println("========Begin sqlTest=========")
    sqlTest(spark, dataframe)

  }

}
