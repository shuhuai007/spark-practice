import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("broadcast test")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    val broadcastArr = (0 until 100).toArray
    val barr1 = sc.broadcast(broadcastArr)
    sc.parallelize(1 to 10, 10).map(_ => barr1.value.length).collect.foreach(println)


//    println(pomDS
//      .withColumn("rowNumber", monotonically_increasing_id())
//      .withColumn("count", udf((s: String) => s.split(" ").size).apply(col("value")))
//      .reduce((a, b) => if (a.getInt(2) > b.getInt(2)) a else b))

  }

}
