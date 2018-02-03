package com.aura.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;

public class BasicPracticeFour {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[1]")
                .setAppName("basicPracticeFour");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO add your code here
        JavaRDD<String> lines = jsc.textFile(args[0]);
        JavaPairRDD<Long, Result> results = lines.mapToPair(new PairFunction<String, Long, Tuple3<Long, Long, Long>>() {
            @Override
            public Tuple2<Long, Tuple3<Long, Long, Long>> call(String line) throws Exception {
                String[] row = line.split(",");
                Long id = Long.parseLong(row[0]);
                Long x = Long.parseLong(row[1]);
                Long y = Long.parseLong(row[2]);
                Long z = Long.parseLong(row[3]);
                return new Tuple2<Long, Tuple3<Long, Long, Long>>(id, new Tuple3(x, y, z));
            }
        }).mapValues(t -> new Result(t._1(), t._2(), t._3(), 1))
                .reduceByKey(Result::add);

        results.collect().forEach(t -> {
            long id = t._1;
            Result r = t._2;
            System.out.printf("ID %d, max(y)=%d, min(z)=%d, avg(x)=%f\n", id, r.maxY, r.minZ, r.avgX());
        });

        jsc.stop();
    }
}

class Result implements Serializable {
    final long sumX;
    final long maxY;
    final long minZ;
    final long countX;
    public Result(long sumX, long maxY, long minZ, long countX) {
        this.sumX = sumX;
        this.maxY = maxY;
        this.minZ = minZ;
        this.countX = countX;
    }
    public double avgX() {
        if (countX != 0) {
            return sumX * 1.0 / countX;
        } else {
            return 0.0;
        }
    }
    public Result add(Result other) {
        return new Result(this.sumX + other.sumX,
                Math.max(this.maxY, other.maxY),
                Math.min(this.minZ, other.minZ),
                this.countX + other.countX);
    }
}
