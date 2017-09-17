package com.youyu.test;

/**
 * Created by root on 2017/5/26.
 */
public class compareTest {
    public static void main(String[] args) {
        String a = "2017-02-01";
        String b = "2017-02-02";
        String c = "2017-02-03";

        if (a.compareToIgnoreCase(b) <= 0) {
            System.out.println("Hello World !");
        }
    }
}
