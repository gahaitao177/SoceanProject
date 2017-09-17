package com.caiyi.spark.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://localhost:3306/spark");
        options.put("dbtable", "student_infos");
        options.put("user", "root");
        options.put("password", "123456");

        Dataset studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "student_scores");

        Dataset studentScoreDF = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 5961090761959007978L;

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(studentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 1197933885537076884L;

            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        JavaRDD<Row> studentsRowRDD = studentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long serialVersionUID = -4274750927142991100L;

            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1(), tuple2._2()._1(), tuple2._2()._2());
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(structFields);
        Dataset studentsDF = sqlContext.createDataFrame(studentsRowRDD, schema);

        /*Row[] rows = studentsDF.collect();
        for (Row row : rows) {
            System.out.println(row);
        }*/

        studentsDF.javaRDD().foreach(new VoidFunction<Row>() {
            private static final long serialVersionUID = 1L;

            public void call(Row row) throws Exception {
                String sql = "insert into good_student_infos values (" + "'" + row.getString(0) + "'," + Integer
                        .valueOf(String.valueOf(row.get(1))) + "," + Integer.valueOf(String.valueOf(row.get(2))) + ")";

                System.out.println("=====================" + sql);

                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement stat = null;
                try {
                    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456");
                    stat = conn.createStatement();
                    stat.executeUpdate(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (stat != null) {
                        stat.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        });
        sc.close();
    }
}
