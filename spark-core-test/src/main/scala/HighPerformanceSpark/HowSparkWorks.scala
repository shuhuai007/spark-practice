package HighPerformanceSpark

import org.apache.spark.sql.SparkSession

object HowSparkWorks {

  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("how spark works")
//      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()


  }

}
