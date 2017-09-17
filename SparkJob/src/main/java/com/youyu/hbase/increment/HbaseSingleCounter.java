package com.youyu.hbase.increment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * http://www.cnblogs.com/similarface/p/5834347.html
 * 单计数器
 * similarface
 * similarface@outlook.com
 */
public class HbaseSingleCounter {
    public static void main(String args[]) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "slave1,slave2,slave3");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("app_user_active"));

        //incrementColumnValue(行号,列族,列,步长)
        long cnt1 = table.incrementColumnValue(Bytes.toBytes("20150105"), Bytes.toBytes("daily"), Bytes.toBytes
                ("hits"), 1L);
        System.out.println(cnt1);

        long cnt2 = table.incrementColumnValue(Bytes.toBytes("20150105"), Bytes.toBytes("daily"), Bytes.toBytes
                ("hits"), 1);
        System.out.println(cnt2);

        long current = table.incrementColumnValue(Bytes.toBytes("20150105"), Bytes.toBytes("daily"), Bytes.toBytes
                ("hits"), 0);
        System.out.println(current);

        long cnt3 = table.incrementColumnValue(Bytes.toBytes("20150105"), Bytes.toBytes("daily"), Bytes.toBytes
                ("hits"), -1);
        System.out.println(cnt3);

        long cnt4 = table.incrementColumnValue(Bytes.toBytes("app#active_user#2017-05-10#com.youyu.yystat#2.4.0#百度"),
                Bytes.toBytes("daily"), Bytes.toBytes
                        ("d01"), 0);
        System.out.println(cnt4);

        table.close();
        connection.close();
    }
}