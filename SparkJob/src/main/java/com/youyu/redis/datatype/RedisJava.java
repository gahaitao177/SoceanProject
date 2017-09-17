package com.youyu.redis.datatype;

import redis.clients.jedis.Jedis;

/**
 * Created by root on 2017/3/15.
 */
public class RedisJava {
    public static void main(String[] args) {
        //连接本地的redis服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server successfully !");
        //查看服务是否运行
        System.out.println("Server is running :" + jedis.ping());

    }
}
