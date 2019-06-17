import org.apache.spark.sql.SparkSession

object MultipleBroadcastTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("broadcast test")
      .master("local[2]")
      .getOrCreate()

    val arr1 = new Array[Int](1000)
    val arr2 = new Array[Int](1000)

    for (i <- 0 until arr1.length) {
      arr1(i) = i
      arr2(i) = i
    }

    val barr1 = spark.sparkContext.broadcast(arr1)
    val barr2 = spark.sparkContext.broadcast(arr2)

    spark.sparkContext.parallelize(1 to 100).map {
      _ => (barr1.value.length, barr2.value.length)
    }
      .collect
      .foreach(println)

  }
}
