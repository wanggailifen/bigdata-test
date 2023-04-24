package com.hoho.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveJdbcDemo {
    public static void main(String[] args) throws Exception {
        //指定hiveserver2的连接
        String jdbcUrl = "jdbc:hive2://192.168.0.11:10000";
        //获取jdbc连接，这里的user使用root，就是linux中的用户名，password随便指定即
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "any");
        //获取Statement
        Statement stmt = conn.createStatement();
        //指定查询的sql
        String sql = "select * from t1";
        //执行sql
        ResultSet res = stmt.executeQuery(sql);
        //循环读取结果
        while (res.next()) {
            System.out.println(res.getInt("id") + "\t" + res.getString("name"));
        }
    }
}