package com.youyu.test;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * Created by User on 2017/8/29.
 */
public class CatTest {
    private static boolean active = true;

    public static void main(String[] args) {
        while (active) {
            Transaction t = Cat.newTransaction("test", "测试中文");
            try {
                Cat.logEvent("测试乱码", "测试乱码");
                Cat.logEvent("测试乱码", "test");
                Cat.logEvent("测试乱码", "test1");
                Cat.logEvent("test", "test");

                t.setStatus(Transaction.SUCCESS);

                Thread.sleep(200);
            } catch (Exception e) {
                t.setStatus(e);
            } finally {
                t.complete();
            }
        }

    }
}
