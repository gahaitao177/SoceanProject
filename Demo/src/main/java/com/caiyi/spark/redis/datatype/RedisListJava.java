package com.caiyi.spark.redis.datatype;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by root on 2017/3/15.
 */
public class RedisListJava {
    public static void main(String[] args) {
        //连接本地的Redis服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server successfully !");

        //存储数据到列表中
        jedis.lpush("tutorial-list", "Redis");
        jedis.lpush("tutorial-list", "Mongodb");
        jedis.lpush("tutorial-list", "Mysql");

        //获取存储的数据并输出
        List<String> list = jedis.lrange("tutorial-list", 0, 5);
        for (int i = 0; i < list.size(); i++) {
            System.out.println("Stored string in redis:" + list.get(i));
        }
    }
}
