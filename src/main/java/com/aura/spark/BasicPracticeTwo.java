package com.aura.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BasicPracticeTwo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[1]")
                .setAppName("basicPracticeTwo");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.setLogLevel("error");
        List<Integer> data = Arrays.asList(1,2,3,4,5, 6);
        JavaRDD<Integer> rdd1 = jsc.parallelize(data, 3);
        List<Integer> data2 =Arrays.asList(7,8,9,10,11);
        JavaRDD<Integer> rdd2 = jsc.parallelize(data2, 2);
        List<Integer> data3=Arrays.asList(12,13,14,15,16, 17, 18, 19, 20, 21);
        JavaRDD<Integer> rdd3 = jsc.parallelize(data3, 3);

        // TODO add your code here
        JavaRDD<Integer> rdd4 = rdd1.union(rdd2);
        System.out.printf("rdd4 has %d partitions\n", rdd4.partitions().size());
        printPartitions(rdd4);
        JavaRDD<Integer> rdd5 = rdd4.coalesce(3);
        System.out.printf("rdd5 has %d partitions\n", rdd5.partitions().size());
        printPartitions(rdd5);
        JavaRDD<Integer> rdd6 = rdd5.repartition(10);
        System.out.printf("rdd6 has %d partitions\n", rdd6.partitions().size());
        printPartitions(rdd6);

        jsc.stop();
    }

    public static void printPartitions(JavaRDD<Integer> rdd) {
        List<List<Integer>> partitions = rdd.glom().collect();
        for(int i=0;i<partitions.size();i++) {
            List<Integer> values = partitions.get(i);
            System.out.println("Partition " + i + " is:" + values);
        }
    }
}
