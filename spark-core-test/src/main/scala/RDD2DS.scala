import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDD2DS {

  private val filePath = "/Users/jiezhou/work/github/spark-practice/spark-core-test/src/main/resources/data/people.txt"
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile(filePath)


    // RDD to Dataset
    import spark.implicits._
    val dataset = rdd.map(line => {
      val arr = line.split(",")
      Person(arr(0), arr(1).trim.toInt)
    }).toDS

    val convertedRdd: RDD[Person] = dataset.rdd



    dataset.printSchema()
  }

}
