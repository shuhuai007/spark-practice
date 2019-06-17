package SparkDefinitiveGuide

import org.apache.spark.sql.SparkSession

object StructuredApiOverView {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("test")
      .master("local[3]")
      .getOrCreate()

    spark.range(100).toDF().collect()

  }

}
