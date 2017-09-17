package com.youyu.hbase;

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
 * 复合计数器
 * similarface
 * similarface@outlook.com
 */
public class HbaseQuery {
    public static void main(String args[]) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "slave1,slave2,slave3");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("app_product_data_daily"));

        long cnt = table.incrementColumnValue(Bytes.toBytes("yy_gjj#new_user#2017-06-13#com.caiyi.fundjs"),
                Bytes.toBytes("data"), Bytes.toBytes
                        ("d01"), 0);

        System.out.println("*********" + cnt);

        table.close();
        connection.close();
    }
}
