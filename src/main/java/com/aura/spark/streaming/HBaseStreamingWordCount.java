package com.aura.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

public class HBaseStreamingWordCount {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("HBaseStreamingWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        JavaDStream<String> lines = ssc.textFileStream(args[0]);
        JavaPairDStream<String, Integer> counts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        counts.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                HTable table = WordCountHBase.getHTable();
                records.forEachRemaining(new Consumer<Tuple2<String, Integer>>() {
                    @Override
                    public void accept(Tuple2<String, Integer> wc) {
                        Put put = WordCountHBase.of(rdd.id(), wc._1, wc._2);
                        try {
                            table.put(put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                table.close();
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

    public static class WordCountHBase {
        private static final String tableName = "wordcount";
        private static final String zkAddress = "bigdata:2181";
        private static final String CF_WORD = "w";

        public static HTable getHTable() throws IOException {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkAddress);
            HTable table = new HTable(conf, TableName.valueOf(tableName));
            return table;
        }

        public static Put of(int id, String word, int count) {
            Put put = new Put(Bytes.toBytes(id));
            put.addColumn(Bytes.toBytes(CF_WORD), Bytes.toBytes(word), Bytes.toBytes(count));
            return put;
        }
    }

}

