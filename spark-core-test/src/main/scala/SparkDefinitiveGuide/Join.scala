package SparkDefinitiveGuide

import org.apache.spark.sql.SparkSession

object Join {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("join test")
      .master("local[2]")
      .getOrCreate()


    import spark.implicits._
    val person = Seq( (0, "Bill Chambers", 0, Seq( 100)), (1, "Matei Zaharia", 1, Seq( 500, 250, 100)), (2, "Michael Armbrust", 1, Seq( 250, 100))).toDF("id", "name", "graduate_program", "spark_status")
    val graduateProgram = Seq( (0, "Masters", "School of Information", "UC Berkeley"), (2, "Masters", "EECS", "UC Berkeley"), (1, "Ph.D.", "EECS", "UC Berkeley")).toDF("id", "degree", "department", "school")
    val sparkStatus = Seq( (500, "Vice President"), (250, "PMC Member"), (100, "Contributor")).toDF("id", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr).explain()
  }

}
