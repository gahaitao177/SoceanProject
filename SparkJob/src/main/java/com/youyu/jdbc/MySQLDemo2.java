package com.youyu.jdbc;

import java.sql.*;

/**
 * http://www.th7.cn/db/mysql/201407/64407.shtml
 * Created by root on 2017/5/9.
 */
public class MySQLDemo2 {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String pwd = "123456";

        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT daycode,d00,d01,d02 from activeuser ");

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(rsmd.getColumnName(i));
            System.out.print("(" + rsmd.getColumnTypeName(i) + ")");
            System.out.print(" | ");
        }

        System.out.println();

        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getString(i) + " | ");
            }

            System.out.println();
        }

        rs.close();
        stmt.close();
        conn.close();
    }
}
