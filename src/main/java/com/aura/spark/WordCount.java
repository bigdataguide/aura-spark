package com.aura.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Word Count");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(x ->
                Arrays.asList(x.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts = words
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Integer> sortedCounts = counts
                .mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));

        sortedCounts.cache();
        System.out.println(sortedCounts.count());
        sortedCounts.take(5).forEach(t -> System.out.println(t._1+":"+t._2));

        jsc.stop();
    }
}
