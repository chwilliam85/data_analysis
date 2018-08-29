package com.cm.data.sql;

import com.cm.data.util.DBUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CMOrderLog {

    public static List<Long> getIDRange(String tableName, String url) {

        String sql = "SELECT MIN(id) as minID, MAX(id) as maxID FROM " + tableName;
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        List<Long> list = null;

        try {
            conn = DBUtils.getConnection(url);
            statement = conn.prepareStatement(sql);
            rs = statement.executeQuery(sql);

            list = new ArrayList<>();
            while (rs.next()) {
                list.add(rs.getLong("minID"));
                list.add(rs.getLong("maxID"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtils.closeAll(conn, statement, rs);
        }

        return list;
    }

    public static int getCount(String tableName, String url) {

        String sql = "SELECT COUNT(1) as ct FROM " + tableName;
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        int count = 0;

        try {
            conn = DBUtils.getConnection(url);
            statement = conn.prepareStatement(sql);
            rs = statement.executeQuery(sql);

            while (rs.next()) {
                count = rs.getInt("ct");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBUtils.closeAll(conn, statement, rs);
        }

        return count;
    }
}
