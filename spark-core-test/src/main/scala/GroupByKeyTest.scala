import org.apache.spark.sql.SparkSession

import scala.util.Random

object GroupByKeyTest {

  val kvPairs = 100
  val byteSize = 100

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("group by key test")
      .master("local[2]")
      .getOrCreate()

    val seedRdd = spark.sparkContext.parallelize(1 until 100, 100)
    val flatMappedRdd = seedRdd.flatMap(p => {
      val arr = new Array[(Int, Array[Byte])](kvPairs)
      val rand = new Random

      for (index <- 0 until kvPairs) {
        val byteArr = new Array[Byte](byteSize)
        rand.nextBytes(byteArr)
        arr(index) = (rand.nextInt(10), byteArr)
      }
      arr
    })

    println("first count: " + flatMappedRdd.count())

    println("count after group by key: " + flatMappedRdd.groupByKey(10).count())
//    println("show after group by key: " + flatMappedRdd.groupByKey(10))

//    flatMappedRdd.groupByKey(10).collect().foreach(println)
    println(flatMappedRdd.groupByKey(10).toDebugString)
  }

}
