package com.aura.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class SqlStreamingWordCount {

    private static SparkSession instance = null;

    public static SparkSession getInstance(SparkConf conf) {
        if (instance == null) {
            instance = SparkSession.builder().config(conf).getOrCreate();
        }
        return instance;
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 2) {
            System.err.println("Usage: SqlStreamingWordCount <hostname> <port>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("StreamingWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        ssc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        words.foreachRDD((rdd,time) -> {
            SparkSession spark = getInstance(rdd.context().getConf());
            JavaRDD<Record> records = rdd.map(w -> new Record(w));
            Dataset<Row> wordsDataFrame =
                    spark.createDataFrame(records, Record.class);
            wordsDataFrame.createOrReplaceTempView("words");
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");
            System.out.printf("========= %d =========\n", time.milliseconds());
            wordCountsDataFrame.show();
        });

        ssc.start();
        ssc.awaitTermination();
    }

}

