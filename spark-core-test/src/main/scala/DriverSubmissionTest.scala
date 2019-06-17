import scala.collection.JavaConverters._


object DriverSubmissionTest {

  def main(args: Array[String]) = {
    val env = System.getenv()
    env.asScala.filter { case (k, _) => k.contains("SPARK_TEST")}.foreach(println)

  }

}
