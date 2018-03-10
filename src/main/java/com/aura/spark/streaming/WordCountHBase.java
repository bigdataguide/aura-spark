package com.aura.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WordCountHBase {

    private static final String zkAddress = "bigdata:2181";
    private static final byte[] CF_WORD = Bytes.toBytes("w");
    private static final byte[] Q_COUNT = Bytes.toBytes("c");

    public static HTable getHTable(String tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddress);
        HTable table = new HTable(conf, TableName.valueOf(tableName));
        return table;
    }

    public static void save(HTable table, String word, int count, long time) throws IOException {
        Put put = new Put(Bytes.toBytes(time));
        put.addColumn(CF_WORD, Bytes.toBytes(word), Bytes.toBytes(count));
        table.put(put);
    }

    public static void incr(HTable table, String word, int count) throws IOException {
        table.incrementColumnValue(Bytes.toBytes(word), CF_WORD, Q_COUNT, count);
    }

}
