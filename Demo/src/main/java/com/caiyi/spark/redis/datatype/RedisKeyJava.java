package com.caiyi.spark.redis.datatype;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by root on 2017/3/15.
 */
public class RedisKeyJava {
    public static void main(String[] args) {
        //连接本地的Redis服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server successfully !");

        //获取数据并输出
        Set<String> set = jedis.keys("*");
        Iterator<String> itt = set.iterator();
        while (itt.hasNext()) {
            System.out.println("Set of stored keys:" + itt.next());
        }
    }
}
