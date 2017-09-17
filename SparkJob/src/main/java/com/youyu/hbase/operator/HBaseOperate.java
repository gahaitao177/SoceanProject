package com.youyu.hbase.operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * http://blog.csdn.net/yan456jie/article/details/51278355
 * Created by root on 2017/3/28.
 */
@SuppressWarnings("deprecation")
public class HBaseOperate {
    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        /*conf.set("hbase.master", "192.168.1.204");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "192.168.1.201");*/
    }


    public static void main(String[] args) throws Exception {
        //表名
        String tableName = "student";
        //列族
        String[] columnFamilys = {"info", "course"};

        //创建的表结构
        createTable(tableName, columnFamilys);

        //向数据表中添加数据
        if (isExists(tableName)) {
            //添加第一行数据
            //表名 行 列族 列 value
            addRow(tableName, "zpc", "info", "age", "20");
            addRow(tableName, "zpc", "info", "sex", "boy");
            addRow(tableName, "zpc", "course", "age", "97");
            addRow(tableName, "zpc", "course", "age", "128");
            addRow(tableName, "zpc", "course", "age", "85");

            //添加第二行数据
            addRow(tableName, "henjun", "info", "age", "19");
            addRow(tableName, "henjun", "info", "sex", "boy");
            addRow(tableName, "henjun", "course", "age", "90");
            addRow(tableName, "henjun", "course", "age", "120");
            addRow(tableName, "henjun", "course", "age", "90");

            //添加第三行数据
            addRow(tableName, "niaopeng", "info", "age", "18");
            addRow(tableName, "niaopeng", "info", "sex", "girl");
            addRow(tableName, "niaopeng", "course", "age", "100");
            addRow(tableName, "niaopeng", "course", "age", "100");
            addRow(tableName, "niaopeng", "course", "age", "99");

            System.out.println("*****************获取一条(zpc)数据*****************");
            getRow(tableName, "zpc");

            System.out.println("*****************修改某个值（value）*****************");
            //incrementColumnValues(tableName, "henjun", "info", "age", 1);

            System.out.println("--------------------------------------------");
            getRow(tableName, "henjun");

            System.out.println("*******************获取所有数据*******************");
            getAllRows(tableName);

            /*System.out.println("***************删除一条(zpc)数据***************");
            delRow(tableName, "zpc");
            getAllRows(tableName);

            System.out.println("****************删除多条数据****************");
            String rows[] = new String[]{"zpc", "henjun"};
            delMultiRow(tableName, rows);
            getAllRows(tableName);

            System.out.println("*****************删除数据库*****************");
            //deleteTable(tableName);
            System.out.println("表" + tableName + "存在吗？" + isExists(tableName));*/
        } else {
            System.out.println(tableName + "此数据库表不存在！！");
        }


    }

    /**
     * 创建数据库表
     *
     * @param tableName
     * @param columnFamilys
     */
    private static void createTable(String tableName, String[] columnFamilys) throws IOException {
        //新建一个数据库管理员
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            System.out.println("表【" + tableName + "】已存在！！");
        } else {
            //新建一个students表的描述
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            //在描述里添加列族
            for (String columFamily : columnFamilys) {
                tableDesc.addFamily(new HColumnDescriptor(columFamily));
            }

            //根据配置好的描述建表
            hAdmin.createTable(tableDesc);
            System.out.println("创建表【" + tableName + "】成功！！");

        }
    }

    /**
     * 删除一个表
     *
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        //新建一个数据库管理员
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            //如果表存在 则需要关闭一个表
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("删除表【" + tableName + "】成功！！");
        } else {
            System.out.println("删除的表【" + tableName + "】不存在 ！！");
            System.exit(0);
        }
    }

    /**
     * 添加一条数据
     *
     * @param tableName
     * @param row
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRow(String tableName, String row, String columnFamily, String column, String value) throws
            IOException {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));

        //参数分别是:列族 列 值
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("插入数据成功！！");
    }

    public static void countRow(String tableName, int nums) {

    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExists(String tableName) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        return hAdmin.tableExists(tableName);
    }

    /**
     * 让表disable
     *
     * @param tableName
     */
    public static void disableTable(String tableName) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        hAdmin.disableTable(tableName);
        System.out.println("表【" + tableName + "】disable，目前不可用！！");
    }

    /**
     * 让表enable
     *
     * @param tableName
     * @throws IOException
     */
    public static void enableTable(String tableName) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        hAdmin.enableTable(tableName);
        System.out.println("表【" + tableName + "】enable，目前可用！！");
    }

    /**
     * 获取一条数据
     *
     * @param tableName
     * @param row
     * @throws IOException
     */
    public static void getRow(String tableName, String row) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                    "\t\t时间戳：" + cell.getTimestamp() + " " +
                    "\t列族名" + new String(CellUtil.cloneFamily(cell)) +
                    "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                    "\t\t值：" + new String(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 获取所有数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                        "\t\t时间戳：" + cell.getTimestamp() + " " +
                        "\t列族名" + new String(CellUtil.cloneFamily(cell)) +
                        "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                        "\t\t值：" + new String(CellUtil.cloneValue(cell)));

            }
        }
    }

    /**
     * 删除一条/行数据
     *
     * @param tableName
     * @param row
     * @throws IOException
     */
    public static void delRow(String tableName, String row) throws IOException {
        HTable table = new HTable(conf, tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    /**
     * 删除多条数据
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void delMultiRow(String tableName, String[] rows) throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> delList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            delList.add(del);
        }
        table.delete(delList);
    }


    // 计数器(amount为正数则计数器加，为负数则计数器减，为0则获取当前计数器的值)
    public static void incrementColumnValues(String tableName, String rowKey, String columnFamily, String column,
                                             long amount) throws Exception {
        HTable table = new HTable(conf, tableName);
        table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes
                (column), amount);

        table.close();

    }

}
