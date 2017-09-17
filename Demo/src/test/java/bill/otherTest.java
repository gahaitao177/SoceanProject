package bill;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class otherTest {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO);//修改日志级别就行了
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("otherTest");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> ilist = new ArrayList<Integer>();
        ilist.add(5);
        ilist.add(5);
        ilist.add(5);

        String aa = "高海涛";

        JavaRDD<Integer> parallelize = jsc.parallelize(ilist);
        System.out.println("-----------|" + parallelize.count() + "aa=" + aa);
        jsc.stop();
    }
}
