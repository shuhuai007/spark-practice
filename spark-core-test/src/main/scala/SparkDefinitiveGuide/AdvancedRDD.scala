package SparkDefinitiveGuide

import org.apache.spark.sql.SparkSession

object AdvancedRDD {

  val collections = "Spark The Definitive Guide : Big Data Processing Made Simple"
  val RETAIL_DATA = "/Users/jiezhou/work/test/scala_test/HSBC/data/online-retail-dataset.csv"

  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("advanced rdd")
      .master("local[2]")
      .getOrCreate()

    val words = spark.sparkContext.parallelize(collections.split(" "), 2)
    // words.collect.foreach(println)

    // rdd -> paired rdd (word, count)
    words.map(word => (word.toLowerCase, 1)).collect().foreach(println)

    // rdd -> paired rdd (w, word)
    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

    // mapValues
    keyword.mapValues(word => word.toUpperCase + "_" + word.length).collect.foreach(println)

    // Aggregation
    words.collect.foreach(println)
    val kvLetter = words.flatMap(word => word.toLowerCase.toSeq).map(letter => (letter, 1))
    kvLetter.foreach(println)
    /*countByKey*/
    kvLetter.countByKey().foreach(println)
    kvLetter.groupByKey.foreach(println)
    /*groupByKey & reduceByKey*/
    kvLetter.groupByKey.map(item => (item._1, item._2.reduce(_ + _))).sortByKey().foreach(println)
    kvLetter.reduceByKey(_ + _).sortByKey().foreach(println)

    // control partitioning
    println("the number of partitions of words:" + words.getNumPartitions)
    /*coalesce*/
    words.coalesce(1).getNumPartitions
    /*repartitioning*/
    words.repartition(10).getNumPartitions
    /*custom partition*/
    val retailDS = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv(RETAIL_DATA)
    retailDS.printSchema()
//    retailDS.rdd.partitio


  }

}
