package com.youyu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * http://blog.csdn.net/u013468917/article/details/52822074
 * Created by Socean on 2017/5/10.
 */
public class HbaseUtils {
    static Configuration conf = null;


    static {
        conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        conf.set(HConstants.ZOOKEEPER_QUORUM, "gs-yy-slave1,gs-yy-slave2,gs-yy-slave3");
    }


    /**
     * 获取所有数据
     *
     * @param tableName
     * @throws IOException
     */
    public static ResultScanner getAllRows(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);

        return results;
    }

    /**
     * 查询表中的所有rowkey
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Map<String, Integer> getAllRowKey(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Map<String, Integer> rowKeyMap = new HashMap<>();

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                String key = new String(CellUtil.cloneRow(cell));
                rowKeyMap.put(key, 1);

            }
        }

        return rowKeyMap;

    }


    /**
     * 根据row范围进行对数据查找
     *
     * @param tableName
     * @param startRow
     * @param endRow
     * @throws IOException
     */
    public static ResultScanner getStartEndRow(String tableName, String startRow, String endRow) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));

        ResultScanner results = (ResultScanner) new Result();

        try {
            results = table.getScanner(scan);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;

    }

    /**
     * 根据rowKey查询
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getResultByRowKey(String tableName, String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table.get(get);

        List<KeyValue> list = result.list();
        for (KeyValue kv : list) {
            System.out.println("列族:" + Bytes.toString(kv.getFamily()));
            System.out.println("列族限定名:" + Bytes.toString(kv.getQualifier()));
            System.out.println("值:" + Bytes.toString(kv.getValue()));
            System.out.println("时间戳:" + kv.getTimestamp());
        }

    }

    /**
     * 判断rowkey是否在表中存在
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Boolean IsExistRowKey(String tableName, String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result r = table.get(get);

        boolean flag = r.isEmpty();

        return flag;
    }

    public static Boolean IsExistRowKey(Connection connection, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result r = table.get(get);

        boolean flag = r.isEmpty();

        return flag;
    }

    /**
     * 计数器(amount为正数则计数器加，为负数则计数器减，为0则获取当前计数器的值)
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param amount
     * @return
     * @throws IOException
     */
    public static Result incrementColumnValues(String tableName, String rowKey, String columnFamily, String column,
                                               long amount) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Increment increment = new Increment(Bytes.toBytes(rowKey));
        increment.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), amount);

        Result result = table.increment(increment);

        return result;
    }

    /**
     * 查询hbase某个值
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param amount
     * @return
     * @throws IOException
     */
    public static Long queryColumnValues(String tableName, String rowKey, String columnFamily, String column,
                                         long amount) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        long count = table.incrementColumnValue(Bytes.toBytes(rowKey),
                Bytes.toBytes(columnFamily), Bytes.toBytes(column), 0);

        return count;
    }

    /**
     * 插入一条数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value) throws
            IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));

        //参数分别是:列族 列 值
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 删除一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);

        System.out.println("删除了一行数据，rowkey为；" + rowKey + ",删除成功！");
    }

    /**
     * 删除指定的某一行中的指定column
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param colunmName
     * @throws IOException
     */
    public static void deleteColumn(String tableName, String rowKey, String familyName, String colunmName) throws
            IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));

        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
        deleteColumn.deleteColumns(Bytes.toBytes(familyName), Bytes.toBytes(colunmName));

        table.delete(deleteColumn);
    }

    /**
     * 查询某一列
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     */
    public static void getColumn(String tableName, String rowKey, String familyName, String columnName) throws
            IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));

        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("列族:" + Bytes.toString(kv.getFamily()));
            System.out.println("列族限定名:" + Bytes.toString(kv.getQualifier()));
            System.out.println("值:" + Bytes.toString(kv.getValue()));
            System.out.println("时间戳:" + kv.getTimestamp());
        }
    }


    /**
     * 更新某行数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void updateColumn(String tableName, String rowKey, String familyName, String columnName, String
            value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
        System.out.println("更新成功");
    }

    public static void filterRowKey(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                //new RegexStringComparator(".11915785#new_user#2017-07-03#867838023524890"));
                //new RegexStringComparator(".锡公积金查询#active_user#2017-07-03#10082#"));
                //new RegexStringComparator(".锡公积金查询#active_user_uniq#2017-07-03#10082#"));
                //new RegexStringComparator(".11915785#active_user_uniq#2017-07-03#867838023524890#"));
                //new RegexStringComparator(".75#active_user#2017-07-03#公积金#中国"));
                //new RegexStringComparator(".11915785#total_user#2017-07-04#867838023524890#"));
                //new RegexStringComparator("锡公积金查询"));
                //new RegexStringComparator("#active_user_uniq#2017-06-24#684d52a8be02d1abfcbebf305d4ac40c#"));
                new RegexStringComparator("yy_jz"));

        scan.setFilter(rowFilter);

        ResultScanner results;

        try {
            results = table.getScanner(scan);

            for (Result result : results) {
                for (Cell cell : result.rawCells()) {

                    //删除匹配的rowkey 然后将其删除
                    //deleteAllColumn(tableName, new String(CellUtil.cloneRow(cell)));

                    System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                            "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                            "\t\t值：" + new String(CellUtil.cloneValue(cell)) /*+
                            "\t\t值：" + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength
                            ())*/);

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
