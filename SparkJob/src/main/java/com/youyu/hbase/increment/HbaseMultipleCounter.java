package com.youyu.hbase.increment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 复合计数器
 * similarface
 * similarface@outlook.com
 */
public class HbaseMultipleCounter {
    public static void main(String args[]) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("app_user_active"));
        Increment increment1 = new Increment(Bytes.toBytes("20160101"));
        increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 1);
        increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
        increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 10);
        increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 10);


        Result result = table.increment(increment1);

        for (Cell cell : result.rawCells()) {
            System.out.println("Cell: " + cell +
                    " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }

        Increment increment2 = new Increment(Bytes.toBytes("20160101"));
        increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 5);
        increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
        increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 0);
        increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), -5);
        Result result2 = table.increment(increment2);
        for (Cell cell : result2.rawCells()) {
            System.out.println("Cell: " + cell +
                    " Value: " + Bytes.toLong(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }

        Increment increment3 = new Increment(Bytes.toBytes("app#data_type#2017-05-10#com.youyu" +
                ".yystat#android-0.1.0_nolog#百度"));
        increment3.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("d01"), 1);


        Result result3 = table.increment(increment3);
        for (Cell cell : result3.rawCells()) {
            System.out.println("Cell: " + cell +
                    " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }

        table.close();
        connection.close();
    }
}
