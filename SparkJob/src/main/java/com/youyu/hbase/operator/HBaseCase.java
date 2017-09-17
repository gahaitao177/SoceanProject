package com.youyu.hbase.operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Socean on 2017/3/28.
 */
@SuppressWarnings("deprecation")
public class HBaseCase {
    static Configuration cfg = HBaseConfiguration.create();

    public static void main(String[] args) {
        String tablename = "priceforcity";//创建表名
        String columnFamily = "base";

        try {
            HBaseCase.create(tablename, columnFamily);
            HBaseCase.put(tablename, "row1", columnFamily, "price", "300");
            HBaseCase.get(tablename, "row1");
            HBaseCase.scan(tablename);
            HBaseCase.delete(tablename);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @param tablename
     * @return
     * @throws IOException
     */
    private static void delete(String tablename) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        System.out.println("删除表成功！！");
    }

    /**
     * @param tablename
     * @throws IOException
     */
    private static void scan(String tablename) throws IOException {
        HTable table = new HTable(cfg, tablename);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        for (Result r : rs) {
            System.out.println("所查看表中的数据： " + r);
        }
    }

    /**
     * @param tablename
     * @param row
     * @throws IOException
     */
    private static void get(String tablename, String row) throws IOException {
        HTable table = new HTable(cfg, tablename);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        System.out.println("按照row所要查询的数据为：" + result);
    }

    /**
     * @param tablename
     * @param row
     * @param columnFamily
     * @param column
     * @param data
     * @throws IOException
     */
    private static void put(String tablename, String row, String columnFamily, String column, String data) throws
            IOException {
        HTable table = new HTable(cfg, tablename);
        Put p1 = new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(p1);
        System.out.println("插入数据： '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
    }

    /**
     * @param tablename
     * @param columnFamily
     * @throws IOException
     */
    private static void create(String tablename, String columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            System.out.println("Table exists !!");
            System.exit(0);
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("创建表成功 !!");
        }
    }


}
