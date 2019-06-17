package ExploringSourceCode

import org.apache.spark.sql.SparkSession

/**
 * Created by jiezhou on 23/11/2018.
 */
object RDDExploration {

  private def f : Int = 5

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rdd exploration")
      .master("local[2]")
      .getOrCreate()


  }

}
