package sourcecode

import java.lang.Long
import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDTest {

  case class Person(name: String)

  def repartitionTest(peopleRDD: RDD[String]): Unit = {
    println(s"the reparition from smaller parition number to bigger:")
    println(s"${peopleRDD.repartition(10).filter(_ => true).toDebugString}")
  }


  def coalesceTest(peopleRDD: RDD[String]): Unit = {
    val coalesceToBiggerPartitonWithoutShuffle = peopleRDD.coalesce(10, false)
    println(s"${coalesceToBiggerPartitonWithoutShuffle.toDebugString}")
    println(s"the coalesce to bigger parition's number : ${coalesceToBiggerPartitonWithoutShuffle.getNumPartitions}") // still 2 partitions
    
    val coalesceToBiggerPartitonWithShuffle = peopleRDD.coalesce(10, true)
    println(s"${coalesceToBiggerPartitonWithShuffle.toDebugString}")
    println(s"the number of paritions after shuffle is :${coalesceToBiggerPartitonWithShuffle.getNumPartitions}")


  }

  def sampleTest(peopleRDD: RDD[String]): Unit = {
    // if one element can be sampled multiple times, yes if true, no if false
    peopleRDD.sample(true, 1).foreach(println)
    peopleRDD.sample(false, 1).foreach(println)
  }


  def randomSplitTest(peopleRDD: RDD[String]): Unit = {
    val rddArr = peopleRDD.randomSplit(Array(0.1, 0.2, 0.7))

    for (i <- 0 until 3) println(s" rdd Arr ${i} size is :${rddArr(i).collect().length}")
  }


  def takeSampleTest(peopleRDD: RDD[String]): Unit = {
    println(s"the size of subset sampled from rdd is ${peopleRDD.takeSample(true, 100).length}")
  }

  def unionTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val rdd1 = spark.sparkContext.parallelize(Seq("good", "morning"))
    val unionRDD = rdd1.union(peopleRDD)
    unionRDD.foreach(println)
    println(unionRDD.toDebugString)
  }

  def distinctTest(peopleRDD: RDD[String]): Unit = {
    peopleRDD.flatMap(line => line.split(",")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    peopleRDD.distinct().foreach(println)
    println(s"the number of partitions about rdd is: ${peopleRDD.getNumPartitions}")
    println(s"the number of partitions about distinct rdd is: ${peopleRDD.distinct().getNumPartitions}")
    println(s"the number of partitions about distinct rdd is: ${peopleRDD.distinct(10).getNumPartitions}")
  }

  def checkpointTest(spark: SparkSession, rdd: RDD[Long]): Unit = {
    spark.sparkContext.setCheckpointDir("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/checkpoint")
    println(s"Is the rdd checkpointed ? : ${rdd.isCheckpointed}")
    rdd.checkpoint()
    //    rdd.collect().foreach(println)
    println(s"get checkpoint file: ${rdd.getCheckpointFile}")
  }

  def sortByTest(peopleRDD: RDD[String]): Unit = {
    val sortByRDD = peopleRDD.sortBy(people => people.split(",")(1).trim.toInt)
    println("sortByRDD's lineage:")
    println(sortByRDD.toDebugString)

    sortByRDD.collect().foreach(println)
  }

  def intersectionTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val people1RDD = spark.sparkContext.parallelize(Seq(
      "how,100,you,do",
      "how,200,you,do",
      "how,300,you,do",
      "how,400,you,do",
      "how are you",
      "how do you do",
      "fine, thank you"
    ))

    val unionRDD = people1RDD.intersection(peopleRDD)
    println("unionRDD's lineage:")
    println(unionRDD.toDebugString)

    unionRDD.collect.foreach(println)
  }

  def glomTest(peopleRDD: RDD[String]): Unit = {
    val glomRDD = peopleRDD.glom()
    glomRDD.foreach(peopleArr => {
      peopleArr.foreach(people => print(s"${people} "))
      println
    })
  }

  def cartesianTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val rdd1 = spark.sparkContext.parallelize(1 until 100)
    rdd1.cartesian(peopleRDD).collect().foreach(println)
  }

  def groupByTest(peopleRDD: RDD[String]): Unit = {
    val groupedRDD = peopleRDD.groupBy(people => people.split(",")(0))
    println("the lineage of grouped rdd is:")
    println(groupedRDD.toDebugString)
    groupedRDD.collect.foreach(println)
  }

  def mapPartitionTest(peopleRDD: RDD[String]): Unit = {
    peopleRDD.mapPartitions(iter => iter.map(e => (e.split(",").length, e))).collect().foreach(println)
    peopleRDD.mapPartitionsWithIndex((index, iter) => iter.map(e => (index + 1, e.split(",").length, e))).foreach(println)
  }

  def foreachPartitionTest(peopleRDD: RDD[String]): Unit = {
    println("foreach partition")
    peopleRDD.foreach(println)
    peopleRDD.foreachPartition(iter => {
      println(iter.toArray.mkString("\t"))
    })
  }


  def collectTest(peopleRDD: RDD[String]): Unit = {
    peopleRDD.collect
    {
      case i: String => i
      case _ => "what is this"
    }
      .foreach(println)
  }

  def treeReduceTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val z = spark.sparkContext.parallelize(List(1,2,3,4,5,6), 2)
    z.foreach(println)
    println(z.reduce(_+_))
  }

  def foldTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val rdd1 = peopleRDD.repartition(3)
    val numRDD = spark.sparkContext.parallelize(1 to 10, 3)
    numRDD.foreach(println)
    println(numRDD.fold(10)((a, b) => a + b))
  }

  def aggregateTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    val numRDD = spark.sparkContext.parallelize(1 to 10, 3)
    println(numRDD.aggregate(10)((a, b) => a + b, (c, d) => c + d))
  }

  def countByValueTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    peopleRDD.flatMap(_.split(",")).countByValue().foreach(map => println(map._1, map._2))
  }

  def takeOrderedTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    peopleRDD.map(_.split(",")(1).toInt).takeOrdered(2).foreach(println)
    peopleRDD.map(_.split(",")(1).toInt).top(2).foreach(println)
  }

  def keyByTest(spark: SparkSession, peopleRDD: RDD[String]): Unit = {
    peopleRDD.keyBy(_.split(",")(0)).foreach(t => println(s"the key is ${t._1}, the value is ${t._2}"))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rdd test")
      .master("local[3]")
      .getOrCreate()

    val rdd = spark.range(1000).rdd
    println(s"the number of partitions for rdd : ${rdd.getNumPartitions}")

    // get rdd's type
    println(s"the class type of Person is ${classOf[Person]}")
    println(s"the class type of rdd is ${rdd.getClass}")

    // checkpoint rdd
    // checkpointTest(spark, rdd)

    val peopleRDD = spark
      .sparkContext
      .textFile("/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt")
    //    println(s"${peopleRDD.toDebugString}")

    // distinct
    // distinctTest(peopleRDD)

    // repartition
    // repartitionTest(peopleRDD)

    // coalesce
    // coalesceTest(peopleRDD)

    // sample
    //     sampleTest(peopleRDD)

    // random split
    //    randomSplitTest(peopleRDD)

    // take sample from rdd with fixed size of element
    //    takeSampleTest(peopleRDD)

    // union
    // unionTest(spark, peopleRDD)

    // sortBy
    // sortByTest(peopleRDD)

    // intersection
    // intersectionTest(spark, peopleRDD)

    // glom
    // glomTest(peopleRDD)

    // cartesian
    // cartesianTest(spark, peopleRDD)

    // groupBy
    // groupByTest(peopleRDD)

    // mapPartitions
    // mapPartitionTest(peopleRDD)

    // foreach partition
    // foreachPartitionTest(peopleRDD)

    // collect
    // collectTest(peopleRDD)

    // tree reduce
    // treeReduceTest(spark, peopleRDD)

    // fold
    // foldTest(spark, peopleRDD)

    // aggregate
    // aggregateTest(spark, peopleRDD)

    // countbyvalue
    // countByValueTest(spark, peopleRDD)

    // takeOrdered
    // takeOrderedTest(spark, peopleRDD)

    // keyBy
    // keyByTest(spark, peopleRDD)



  }
}
