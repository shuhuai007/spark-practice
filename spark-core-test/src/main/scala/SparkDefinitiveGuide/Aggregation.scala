package SparkDefinitiveGuide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Aggregation {

  private val RETAIL_DATA = "/Users/jiezhou/work/test/scala_test/HSBC/data/online-retail-dataset.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("aggregation test")
      .master("local[2]")
      .getOrCreate()

    var df = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(RETAIL_DATA)
      .coalesce(5)

    df.cache()
    df.createOrReplaceTempView("retail")


    // group
    df.printSchema()
    df.groupBy("InvoiceNo", "CustomerID").count().show()
    df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan"), expr("count(Quantity)")).show()
    df.groupBy("InvoiceNo").agg("Quantity" -> "count", "Quantity" -> "avg").show()

    // window spec
    df = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    val windowSpec = Window.partitionBy("CustomerID", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchageRank = rank().over(windowSpec)
    val purchageDenseRank = dense_rank().over(windowSpec)
    val rowNumber = row_number().over(windowSpec)
    df.where("CustomerID IS NOT NULL").orderBy("CustomerID")
      .select(
        col("CustomerID"),
        col("date"),
        col("Quantity"),
        purchageRank.alias("quanRank"),
        purchageDenseRank.alias("quanDenseRank"),
        rowNumber.alias("rowNumber"),
        maxPurchaseQuantity.alias("maxQuan")
        ).show()

  }

}
