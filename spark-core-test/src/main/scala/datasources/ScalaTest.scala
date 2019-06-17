package datasources

import org.apache.spark.sql.SparkSession

object ScalaTest {
  def test1(a: String) = {
    (a, a.length)
  }

  def test2(a: String): Option[(String, Int)] = {
    Some(a, a.length)
  }

  def main(args: Array[String]): Unit = {

//    val str = "hello, world, zhoujie"
//    val strList = str.split(",").map(_.trim).toList
//    println(strList)
//
//    val test = List(1, 20, 30, 40)
//    println(test.foldLeft(10)(_ + _))
//
//    val testOption = Option("good")
//    println(testOption.map(_ + "A"))
//
//    val name = Option("Alex")
//    val age = Option(100)
//
//    val name_age = for (n <- name; a <- age) yield a + "_" + n
//    println(name_age)

    val testStr = "hello, world, good moring gooda afternoon"

    println("goodaa".r.findAllMatchIn(testStr).size)
    println("good".r.findAllMatchIn(testStr))

    val a = "goodaa".r.findAllMatchIn(testStr)

//    a match {
//      case m :: _ => println("is not empty")
//      case _ => println("empty list")
//    }

    val b = "goodaa".r.findAllIn(testStr).toList
    println(b)


  }

}
