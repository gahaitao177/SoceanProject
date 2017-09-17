package com.youyu.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by root on 2017/6/23.
 */
public class QueryHbaseStartEnd {
    public static void main(String[] args) throws IOException {

        System.exit(0);

        String startKey = "yy_gjj#Home_account_Binding#2017-07-30";
        String endKey = "yy_gjj#Home_account_Binding#2017-08-06";

        ResultScanner results = HbaseUtils.getStartEndRow("app_product_event_data_weekly", startKey, endKey);

        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                        "\t\t值：" + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            }
        }
    }
}
