package com.youyu.hbase;

import java.io.IOException;

/**
 * Created by root on 2017/8/14.
 */
public class DeleteHbaseColumn {
    public static void main(String[] args) throws IOException {
        /**
         * 删除某行的某一列数据
         */
        String tableName = "tmp_event_dic";
        String rowKey = "jz_new";
        String familyName = "info";
        String columnName = "addRecord_memo";

        HbaseUtils.deleteColumn(tableName, rowKey, familyName, columnName);
    }
}
