import org.apache.spark.sql.SparkSession

object HDFSTest {

  val FILTPATH = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val file = spark.read.textFile(FILTPATH).rdd
    file.map(line => line.length).collect.foreach(println)
  }

}
