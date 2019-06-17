import org.apache.spark.sql.SparkSession

object WordCountTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("word count test")
      .master("local[2]")
      .getOrCreate()

    val filePath = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt"
    val wordRdd = spark.sparkContext.textFile(filePath).flatMap(line => line.split(","))
    wordRdd.map(word => (word, 1)).countByKey().foreach(println)
//    wordRdd.map(word => (word, 1)).reduceByKey(_ + _).collect().foreach(println)

//    val rdd = spark.sparkContext.textFile(filePath)
//    rdd.flatMap(_.split(",")).map(word => (word.trim, 1)).reduceByKey((a, b) => a + b).collect().foreach(println)
//
//    rdd.mapPartitionsWithIndex((index, iterator) => iterator.map(_.concat("_" + index))).collect().foreach(println)
  }

}
