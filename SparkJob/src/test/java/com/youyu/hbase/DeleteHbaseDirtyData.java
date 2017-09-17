package com.youyu.hbase;

import java.io.IOException;

/**
 * Created by root on 2017/7/5.
 */
public class DeleteHbaseDirtyData {
    public static void main(String[] args) throws IOException {
        //删除某行脏数据
        /*HbaseUtils.deleteAllColumn("app_product_data_daily", "ios_start" +
                "11915785#active_user#2017-07-03#867838023524890#");

        System.exit(1);*/

        //模糊查询出来的rowkey 然后将匹配出来的数据进行删除
        HbaseUtils.filterRowKey("event_dic");

        System.exit(1);

    }
}
