package SparkDefinitiveGuide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WorkingWithDataType {

  case class Person(name: String, age: Int, address: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("data type test")
      .master("local[2]")
      .getOrCreate

    val personSeq = Seq(
      Person("zhou jie lun", 40, "TaiBei"),
      Person("zhou xing chi", 55, "HongKong"),
      Person(null, 55, "HongKong"),
      Person("zhang zi yi", 44, "BeiJing")
    )

    import spark.implicits._
    val personDs = spark.createDataset(personSeq)
    personDs.show()

    personDs.select(lit(5), lit("how r u")).show()

    personDs.where(col("name").equalTo("zhoujielun")).show()
    personDs.where("age = 55").show()

    // initcap
    personDs.select(initcap(col("name"))).show()

    // uppercase/lowercase/trim/regex/date-time
    personDs.select(upper(col("name"))).show(4)
    personDs.select(lower(col("name"))).show(4)
    personDs.select(trim(col("name"))).show(4)
    personDs.select(regexp_replace(col("name"), "zhou", "wang")).show(4)

    // coalesce
    personDs.select(coalesce(col("name")), col("age")).show()

    // drop rows with null value
    personDs.na.drop().show()

    // complex data
    /*struct*/
    personDs.selectExpr("(name, age) as name_age").select(col("name_age").getField("name")).show()
    /*array*/
    personDs.select(split(col("name"), " ").alias("nameArr")).selectExpr("nameArr[0]").show()
    personDs.select(split(col("name"), " ").alias("nameArr")).withColumn("arr_len", size(col("nameArr"))).show()
    /*array explode*/
    personDs.where(col("name").equalTo("zhou jie lun"))
      .withColumn("splitted", split(col("name"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("exploded","name", "age", "address")
      .show()


    // udf
    /*register udf as a dataframe function*/
    val numDF = spark.range(5).toDF("num")
    def sum3(item: Int): Int = (1 to 3).map(_ => item).reduce(_ + _)
    println(sum3(10))
    val sum3udf = spark.udf.register("sum3", sum3(_:Int):Int)

    numDF.select(sum3udf(col("num"))).show()

    /*register udf as a spark sql function*/
    spark.udf.register("sum3", sum3(_:Int):Int)
    numDF.selectExpr("sum3(num)").show()


  }
}
