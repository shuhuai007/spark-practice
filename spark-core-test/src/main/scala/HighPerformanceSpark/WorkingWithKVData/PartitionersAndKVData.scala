package HighPerformanceSpark.WorkingWithKVData

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartitionersAndKVData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("partitioner test")
      .master("local[2]")
      .getOrCreate()

    val pairRDD = spark.sparkContext.parallelize(
      Seq(
        (1, "z"),
        (2, "d"),
        (3, "dsf"),
        (4, "sdfsd"),
        (5, "rr"),
        (6, "z"),
        (7, "zfg"),
        (8, "zbb"),
        (9, "zoo"))
    , 3)

    println("partitioner of pairRDD is empty : " + pairRDD.partitioner.isEmpty)

    val partitionedRDD = pairRDD.partitionBy(new HashPartitioner(8))
    println("partitioner of partitionedRDD is empty : " + partitionedRDD.partitioner.isEmpty)

    // mapValue keep the partitioner
    val mapValueRDD = partitionedRDD.mapValues(name => name + "_")
    println("partitioner of mapValueRDD is empty : " + mapValueRDD.partitioner.isEmpty)

    // map can't keep the partitoner
    val mapRDD = partitionedRDD.map(elem => (elem._1, elem._2 + "--"))
    println("partitioner of mapRDD is empty : " + mapRDD.partitioner.isEmpty)



  }

}
