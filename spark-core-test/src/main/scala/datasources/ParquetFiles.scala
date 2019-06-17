package datasources

import org.apache.spark.sql.SparkSession

/**
 * Created by jiezhou on 23/11/2018.
 */
object ParquetFiles {

  def main (args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("parquet files")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.parallelize(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.show()
    squaresDF.write.parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.show()
    cubesDF.write.parquet("data/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.show()
    mergedDF.printSchema()
    mergedDF.repartition()
  }

}
