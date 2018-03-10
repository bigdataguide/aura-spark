package com.aura.spark.streaming;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

public class HBaseStreamingWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("HBaseStreamingWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
        JavaPairDStream<String, Integer> counts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        counts.foreachRDD((rdd, time) -> {
            rdd.foreachPartition(records -> {
//                HTable t1 = WordCountHBase.getHTable("streaming_word_count");
                HTable t2 = WordCountHBase.getHTable("streaming_word_count_incr");
                records.forEachRemaining(new Consumer<Tuple2<String, Integer>>() {
                    @Override
                    public void accept(Tuple2<String, Integer> wc) {
                        try {
//                            WordCountHBase.save(t1, wc._1, wc._2, time.milliseconds());
                            WordCountHBase.incr(t2, wc._1, wc._2);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
//                t1.close();
                t2.close();
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

}

