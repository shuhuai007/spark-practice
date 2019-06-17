package HighPerformanceSpark.author.wordcount

/**
 * What sort of big data book would this be if we didn't mention wordcount?
 */
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

object WordCount {
  // bad idea: uses group by key
  def badIdea(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val grouped = wordPairs.groupByKey()
    val wordCounts = grouped.mapValues(_.sum)
    wordCounts
  }

  // good idea: doesn't use group by key
  //tag::simpleWordCount[]
  def simpleWordCount(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey((a, b) => a + b)
    wordCounts
  }
  //end::simpleWordCount[]

  /**
    * Come up with word counts but filter out the illegal tokens and stop words
    */
  //tag::wordCountStopwords[]
  def withStopWordsFiltered(rdd : RDD[String], illegalTokens : Array[Char],
    stopWords : Set[String]): RDD[(String, Int)] = {
    val separators = illegalTokens ++ Array[Char](' ')
    val tokens: RDD[String] = rdd.flatMap(_.split(separators).
      map(_.trim.toLowerCase))
    val words = tokens.filter(token =>
      !stopWords.contains(token) && (token.length > 0) )
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }
  //end::wordCountStopwords[]


  def main(args: Array[String]): Unit = {
    println("hello, world")
    val spark = SparkSession
      .builder()
      .appName("word count test")
      .master("local[2]")
      .getOrCreate()

    val text = Seq("how are you today? where are you from?",
      "I come from Xi'an city,which is in the northwest of China",
      "nice to meet you",
      "what's your favorite book?")
    val words = spark.sparkContext.parallelize(text, 5)

    // word count via groupbykey
    val wordPairs = words.flatMap(_.split(" ")).map(word => (word.trim.toLowerCase, 1))
    val grouped = wordPairs.groupByKey()
    val wordCount = grouped.mapValues(_.sum)
    wordCount.foreach(pair => println(pair._1 + ":" + pair._2))

    // word count via reducebykey
    wordPairs.reduceByKey(_ + _)

  }
}
