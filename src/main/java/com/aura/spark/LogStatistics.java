package com.aura.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LogStatistics {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[1]")
                .setAppName("LogStatistics");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Accumulator<Integer> total = jsc.accumulator(0);
        Accumulator<Integer> count400 = jsc.accumulator(0);
        Accumulator<Integer> count200 = jsc.accumulator(0);

        JavaRDD<String> input = jsc.textFile(args[0]);
        input.foreach(s -> {
            String[] row = s.split(",");
            int code = Integer.parseInt(row[0]);
            if (code == 400) {
                count400.add(1);
            } else if (code == 200) {
                count200.add(1);
            }
            total.add(1);
        });

        System.out.println("Total:" + total.value());
        System.out.println("400:" + count400.value());
        System.out.println("200:" + count200.value());
        jsc.stop();
    }
}
