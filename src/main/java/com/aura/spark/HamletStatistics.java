package com.aura.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral;
import scala.Function1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HamletStatistics {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[1]")
                .setAppName("HamletStatistics");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> stopwords = jsc.textFile(args[0]);
        // 1.collect stopwords and broadcast to executors
        Set<String> stopwordSet = new HashSet<String>();
        stopwordSet.addAll(stopwords.collect());
        Broadcast<Set<String>> broadcastedStopWords = jsc.broadcast(stopwordSet);
        // 2.define accumulators, countTotal and stopTotal
        Accumulator<Integer> countTotal = jsc.accumulator(0);
        Accumulator<Integer> stopTotal = jsc.accumulator(0);

        JavaRDD<String> input = jsc.textFile(args[1]);
        JavaRDD<String> words = input.flatMap(s -> splitWords(s).iterator());
        JavaRDD<String> filteredWords = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                // TODO 3.filter stop words, increase countTotal or stopTotal
                countTotal.add(1);
                boolean isStopWord = broadcastedStopWords.value().contains(v1);
                if (isStopWord) {
                    stopTotal.add(1);
                }
                return !isStopWord;
            }
        });

        // calculate result
        JavaPairRDD<String, Integer> counts = filteredWords
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        // sort result
        JavaPairRDD<Integer, String> sortedCounts = counts
                .mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
                .sortByKey(false);

        // output result
        System.out.println("Total words:" + countTotal.value());
        System.out.println("Stop words:" + stopTotal.value());
        sortedCounts.take(10).forEach(t -> {
            System.out.printf("word: %s,count: %d\n",t._2,t._1);
        });

        jsc.stop();
    }

    public static List<String> splitWords(String line) {
        List<String> result = new ArrayList<String>();
        String[] words = line.replaceAll("['.,:?!-]", "").split("\\s");
        for (String w: words) {
            if (!w.trim().isEmpty()) {
                result.add(w.trim());
            }
        }
        return result;
    }
}
