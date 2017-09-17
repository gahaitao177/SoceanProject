package com.youyu.jdbc;

import java.sql.*;

/**
 * Created by root on 2017/5/8.
 */
public class MySQLDemo1 {
    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSetMetaData m = null;
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String pwd = "123456";

        try {
            conn = DriverManager.getConnection(url, user, pwd);

            String sql1 = "select * from wordcount";
            stmt = conn.prepareStatement(sql1);

            ResultSet rs = stmt.executeQuery();

            m = rs.getMetaData();
            int columns = m.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    System.out.print(rs.getString(i) + " ");
                }
                System.out.println();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
