package SparkDefinitiveGuide

import org.apache.spark.sql.SparkSession

object RDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("rdd test")
      .master("local[3]")
      .getOrCreate()

    val wordArr = "how are you. could you say more about NBA?".split(" ")

    val words = spark.sparkContext.parallelize(wordArr, 2)

    // map
    words.map(_ + "M").collect.foreach(println)

    // mapPartition
    words.mapPartitions(arr => {
      arr.map(str => str + "M")
    }, true).collect.foreach(println)

    // glom
    words.glom().collect.foreach(arr => println(arr.mkString(" ")))



  }

}
