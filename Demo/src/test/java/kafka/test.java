package kafka;

import java.util.Random;

/**
 * Created by root on 2017/3/16.
 */
public class test {
    public static void main(String[] args) {
        String[] arr = {"zhangsan,1", "lisi,1", "wangwu,1", "xiaming,1"};
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int flag = random.nextInt(4);
            System.out.println(flag + "  " + arr[flag]);
        }
    }
}
