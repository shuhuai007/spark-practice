package HighPerformanceSpark

import org.apache.spark.sql.SparkSession

object Joins {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("join test")
      .master("local[2]")
      .getOrCreate()

    val rdd1 = spark.sparkContext.parallelize(List(
      (1, "zhouenlai"),
      (2, "maozedong"),
      (3, "ee"),
      (4, "d"),
      (5, "c"),
      (6, "b"),
      (7, "z"),
      (8, "h"),
      (9, "i"),
      (10, "j")), 3)

    println(rdd1.getNumPartitions)
    println("partitioner is empty? " + rdd1.partitioner.isEmpty)
    println("partitioner is empty? " + rdd1.repartition(2).partitioner.isEmpty)
    println("partitioner is empty? " + rdd1.coalesce(2).partitioner.isEmpty)

    rdd1.keyBy(pair => pair._1).foreach(println)
    val pairRDD = rdd1.keyBy(pair => pair._1).coalesce(2)
    println("pair rdd, partitioner is empty? " + pairRDD.partitioner.isEmpty)

  }

}
