package com.youyu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by root on 2017/5/8.
 */
public class QueryHbaseData {
    public static void main(String[] args) throws IOException {
        /*HbaseUtils.deleteAllColumn("app_product_data_daily", "�|�|Wf�p\u0002��m[��ҙr{NV\u0012{�)" +
                "11915785#active_user#2017-07-03#867838023524890#");

        System.exit(1);*/

        /*HbaseUtils.filterRowKey("version_and_channel_dic");

        System.exit(1);*/

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "gs-yy-slave1,gs-yy-slave2,gs-yy-slave3");

        Connection connection = ConnectionFactory.createConnection(configuration);

        Table table = connection.getTable(TableName.valueOf("tag_user_info_pre"));

        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t\t时间戳：" + cell.getTimestamp() + " " +
                        "\t列族名: " + new String(CellUtil.cloneFamily(cell)) +
                        "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                        "\t\t值：" + new String(CellUtil.cloneValue(cell)));

            }
        }

        /*for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t\t时间戳：" + cell.getTimestamp() + " " +
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

        table.close();
        connection.close();
    }
}
