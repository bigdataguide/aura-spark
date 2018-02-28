package com.aura.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Option;
import scala.Tuple2;

import java.util.Arrays;

public class StatefulStreamingWordCount {

    public static JavaStreamingContext createContext(String checkPointDir, String host, int port) {
        SparkConf sparkConf = new SparkConf().setAppName("StatefulStreamingWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
        ssc.checkpoint(checkPointDir);
        ssc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                host, port, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordPairs = words
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1));

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateWordCounts =
                wordPairs.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) throws Exception {
                        Option<Integer> stateCount = state.getOption();
                        Integer sum = one.orElse(0);
                        if (stateCount.isDefined()) {
                            sum += stateCount.get();
                        }
                        state.update(sum);
                        return new Tuple2<String, Integer>(word, sum);
                    }
                }));

        stateWordCounts.print();
        stateWordCounts.stateSnapshots().print();
        return ssc;
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: StatefulStreamingWordCount <check_dir> <hostname> <port>");
            System.exit(1);
        }
        String checkPointDir = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        // Create the context with a 1 second batch size
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkPointDir, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return createContext(checkPointDir, host, port);
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
