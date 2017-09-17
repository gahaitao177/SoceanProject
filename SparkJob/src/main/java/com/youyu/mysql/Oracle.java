package com.youyu.mysql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

/**
 * Created by root on 2017/9/14.
 */
public class Oracle {
    private static final Object ORACLE_USERNAME = "credit";
    private static final Object ORACLE_PWD = "credit";
    private static final String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:@192.168.1.193:1521:lottery";
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReadMyOracle").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", ORACLE_USERNAME);
        connectionProperties.put("password", ORACLE_PWD);
        connectionProperties.put("driver", ORACLE_DRIVER);

        final String dbTable = "select * from bk_acctbook_data";
        DataFrame jdbcDF = sqlContext.read().jdbc(ORACLE_CONNECTION_URL, dbTable, connectionProperties);

        jdbcDF.show();

        sc.stop();
    }
}
