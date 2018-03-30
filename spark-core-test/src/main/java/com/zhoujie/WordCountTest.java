package com.zhoujie;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCountTest {
    private static final Pattern SPACE = Pattern.compile(";");

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("WordCount Test").setMaster("local[2]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(getResourceDataFile());

        JavaPairRDD<String, Integer> counts = lines
                .flatMap(s -> Arrays.asList(SPACE.split(s)))
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }

    private static String getResourceDataFile() {
        return WordCountTest.class.getResource("/data/people.txt").getPath();
    }

}
