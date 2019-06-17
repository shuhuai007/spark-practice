package datasources

import org.apache.spark.sql.{SaveMode, SparkSession}

object GenericLoadSaveFunction {

  val baseDir = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data"
  val userParquetFilePath = s"${baseDir}/users.parquet"
  val peopleJSONFilePath = s"${baseDir}/people.json"
  val peopleCSVFilePath = s"${baseDir}/people.csv"

  def main (args: Array[String]){
    val spark = SparkSession.builder()
      .appName("generic load and save function")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    // by default, load will read parquet file if the format is not specified
    val userDF = spark.read.load(userParquetFilePath)
    userDF.printSchema()
    userDF.show()

    // load json file
    val peopleJSONDF = spark.read.format("json").load(peopleJSONFilePath)
    peopleJSONDF.printSchema()
    peopleJSONDF.show()
    // write into parquet file
    peopleJSONDF
      .select($"name", $"age")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${baseDir}/output-parquet")

    // load csv file
    val peopleCSVDF = spark
      .read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("sep", ";")
      .load(peopleCSVFilePath)
    peopleCSVDF.printSchema()
    peopleCSVDF.show()
    // write into orc file
    peopleCSVDF
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .save(s"${baseDir}/output-orc")

    // save as a table
    peopleCSVDF.write.bucketBy(3, "name").sortBy("age").saveAsTable("people_bucket_tb")

    userDF.write.mode(SaveMode.Overwrite).partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    userDF
      .write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")
  }

}
