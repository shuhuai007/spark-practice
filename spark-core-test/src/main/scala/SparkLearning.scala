import org.apache.spark.sql.SparkSession

object SparkLearning {
  val FILE_PATH = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("First App")
      .master("local[2]")
      .getOrCreate()

    val file = spark.read.textFile(FILE_PATH).rdd
    val mapped = file.map(line => line.length).cache()
    mapped.collect().foreach(println)

    for (i <- 1 to 10) {
      val start = System.currentTimeMillis()
      for (x <- mapped) { x + 2 }

      val end = System.currentTimeMillis()

      println(s"Iteration $i took ${end-start} ms")
    }

  }

}
