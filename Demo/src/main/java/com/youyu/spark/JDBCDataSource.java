package com.youyu.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by root on 2016/11/25.
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://192.168.2.180:3306/test");
        options.put("dbtable", "student_infos");
        options.put("user", "root");
        options.put("password", "123456");

        Dataset studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "student_scores");
        Dataset studentScoresDF = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String, Tuple2<Integer, Integer>> studentInfosRDD = studentInfosDF.javaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    public Tuple2<String, Integer> call(Row row) throws
                            Exception {
                        return new Tuple2<String, Integer>(row
                                .getString(0),
                                Integer.valueOf(String.valueOf(row
                                        .get(1))));
                    }
                }).join(studentScoresDF.toJavaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    public Tuple2<String, Integer> call(Row row) throws
                            Exception {
                        return new Tuple2<String, Integer>(row
                                .getString(0),
                                Integer.valueOf(String.valueOf(row
                                        .get(1))));
                    }
                }));

        JavaRDD<Row> studentInfosRowRDD = studentInfosRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>,
                Row>() {
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
                    throws Exception {
                return new RowFactory().create(tuple._1(), tuple._2()._1(),
                        tuple._2()._2());
            }
        }).filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {

                if (row.getInt(2) > 80) {
                    return true;
                }
                return false;
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(structFields);

        Dataset studentDF = sqlContext.createDataFrame(studentInfosRowRDD, schema);
    }
}
