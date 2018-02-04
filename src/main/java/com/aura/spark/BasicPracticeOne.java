package com.aura.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class BasicPracticeOne {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[1]")
                .setAppName("basicPracticeOne");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.setLogLevel("error");
        List<Integer> result = new ArrayList();
        for(int i = 100; i <= 1000; i++) result.add(i);
        JavaRDD<Integer> input = jsc.parallelize(result, 7);


        // TODO add your code here
        List<Integer> head5 = input.take(5);
        System.out.println("head 5 elements are:" + head5);
        int total = input.reduce((x, y) -> x + y);
        System.out.println("sum is:" + total);
        double avg = total * 1.0/input.count();
        System.out.println("avg is:" + avg);
        JavaRDD<Integer> evens = input.filter(x -> x%2 == 0);
        System.out.println("count of even number is:" + evens.count());
        System.out.println("head 5 of even number is:" + evens.take(5));

        jsc.stop();
    }
}
