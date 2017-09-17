package com.youyu.test;

import com.youyu.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by root on 2017/5/8.
 */
public class QueryHbaseData {
    public static void main(String[] args) throws IOException {
        /*Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "gs-yy-slave1,gs-yy-slave2,gs-yy-slave3");

        Connection connection = ConnectionFactory.createConnection(configuration);

        Table table = connection.getTable(TableName.valueOf("app_data_daily"));*/

        String startKey = "yy_gjj#new_user#2017-06-13#com.caiyi.fundjs#";
        String endKey = "yy_gjj#new_user#2017-06-13#com.caiyi.fundjs#1";

        ResultScanner results = HbaseUtils.getStartEndRow("app_data_daily", startKey, endKey);

        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t\t时间戳：" + cell.getTimestamp() + " " +
                        "\t列族名: " + new String(CellUtil.cloneFamily(cell)) +
                        "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                        "\t\t值：" + new String(CellUtil.cloneValue(cell)));

            }
        }

        /*Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t\t时间戳：" + cell.getTimestamp() + " " +
                        "\t列族名: " + new String(CellUtil.cloneFamily(cell)) +
                        "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                        "\t\t值：" + new String(CellUtil.cloneValue(cell)));

            }
        }*/

        /*for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                        "\t\t值：" + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            }
        }*/

        /*for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell +
                        " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }*/

        /*table.close();
        connection.close();*/
    }
}
