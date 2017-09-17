package com.youyu.redis.datatype;

import redis.clients.jedis.Jedis;

/**
 * Created by root on 2017/3/15.
 */
public class RedisStringJava {
    public static void main(String[] args) {
        //连接本地的Redis服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server successfully !");

        //设置redis字符串数据
        jedis.set("w2ckey", "Redis tutorials");
        //获取存储数据并输出
        System.out.println("Stored string in redis:" + jedis.get("w2ckey"));
    }
}
