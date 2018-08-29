package com.cm.data.util;

import java.sql.*;

public class DBUtils {

    final private static String DRIVER = "com.mysql.cj.jdbc.Driver";
    final private static String USER = System.getProperty("db_user");
    final private static String PASSWORD = System.getProperty("db_password");

    public static Connection getConnection(String url) {
        Connection conn = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(url,USER,PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void closeAll(Connection conn, Statement pStmt, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (pStmt != null) {
            try {
                pStmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
