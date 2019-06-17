import SparkSQLExample.Person
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSchemaTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("spark schema test")
      .master("local[2]")
      .getOrCreate()

    val schema = StructType(List(StructField("name", StringType, false), StructField("age", IntegerType, false)))

    val personRDD = spark.sparkContext.textFile("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt")

    val personDF = spark.createDataFrame(personRDD.map(person => Row(person.split(",")(0), person.split(",")(1).trim.toInt)), schema)
    personDF.show()

    import spark.implicits._
    val personDS = personDF.as[Person]
    personDS.show(2)


  }
}
