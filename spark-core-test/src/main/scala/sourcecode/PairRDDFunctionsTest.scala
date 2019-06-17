package sourcecode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by jiezhou on 25/11/2018.
 */
object PairRDDFunctionsTest {

  def combineByKeyWithClassTagTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val pairRDD = peopleRDD.map(people => (people.split(",")(0), people)).repartition(4)
    println(s"the number of partitions about pairRDD is: ${pairRDD.getNumPartitions}")
    pairRDD.foreachPartition(partition => println(partition.length))
    pairRDD.mapPartitionsWithIndex((index, iter) => iter.map(index + "_" + _.toString())).foreach(println)

    val createCombiner = (people: String) => (people.split(",")(1).trim.toInt, 1)
    val mergeValue = (cc: (Int, Int), people: String) => (cc._1 + people.split(",")(1).trim.toInt, cc._2 + 1)
    val mergeCombiner = (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    pairRDD.combineByKey(createCombiner, mergeValue, mergeCombiner).mapValues(v => v._1/v._2).foreach(println)
  }

  def aggregateByKeyTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val peoplePairRDD = peopleRDD.map(people => (people.split(",")(0).trim, people)).repartition(4)
    peoplePairRDD.foreach(println)

    // average age of each one
    val seqOp = (cc: (Int, Int), people: String) => (cc._1 + people.split(",")(1).trim.toInt, cc._2 + 1)
    val combinerOp = (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    peoplePairRDD.aggregateByKey((0, 0))(seqOp, combinerOp).mapValues(v => v._1/v._2).foreach(println)

  }

  def foldByKeyTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val peoplePairRDD = peopleRDD.map(people => (people.split(",")(0).trim, people)).repartition(4)
    peoplePairRDD.foldByKey("M")((a, b) => a + "A" + b).foreach(println)
  }

  def reduceByKeyTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val peoplePairRDD = peopleRDD.map(people => (people.split(",")(0).trim, people)).repartition(4)
    peoplePairRDD.reduceByKey((a, b) => a + "A" + b).foreach(println)
  }


  def joinTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val peoplePairRDD = peopleRDD.map(people => (people.split(",")(0).trim, people.split(",")(1).trim.toInt)).repartition(4)
    val rdd2 = peoplePairRDD.filter(pair => pair._2 > 100).groupByKey().mapValues(iter => iter.toSeq(0))
    println("rdd2 begin------")
    rdd2.foreach(println)
    println("rdd2 end------")
    peoplePairRDD.join(rdd2).foreach(println)
  }

  def cogroupTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val peoplePairRDD = peopleRDD.map(people => (people.split(",")(0).trim, people.split(",")(1).trim.toInt)).repartition(4)
    val rdd2 = peoplePairRDD.filter(pair => pair._2 > 100).groupByKey().mapValues(iter => iter.toSeq(0))

    peoplePairRDD.cogroup(rdd2).foreach(println)
  }

  def lookupTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    println("lookup begin")
    println(peopleRDD.map(people => (people.split(",")(0), people)).lookup("Tom"))
    println("lookup end")
  }

  def main (args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("pair rdd functions")
      .master("local[3]")
      .getOrCreate()

    val peopleRDD = spark.sparkContext.textFile("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt")
    // combineByKeyWithClassTag
    // combineByKeyWithClassTagTest(spark, peopleRDD)

    // aggregateByKey
    // aggregateByKeyTest(spark, peopleRDD)

    // foldByKey
    // foldByKeyTest(spark, peopleRDD)

    // reduceByKey
    // reduceByKeyTest(spark, peopleRDD)

    // join
    // joinTest(spark, peopleRDD)

    // cogroup
    // cogroupTest(spark, peopleRDD)

    // lookup
    lookupTest(spark, peopleRDD)

  }
}
