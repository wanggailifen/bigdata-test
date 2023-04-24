package com.hoho.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.eclipse.jdt.internal.compiler.classfmt.MethodInfoWithAnnotations;

import java.io.IOException;

public class HBaseConnect {
    // 设置静态属性 hbase 连接
    public static Connection connection = null;

    static {
        // 创建 hbase 的连接
        try {
            // 使用配置文件的方法
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
    }

    /**
     * 连接关闭方法,用于进程关闭时调用
     *
     * @throws IOException
     */
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(HBaseConnect.connection);

        HBaseConnect.closeConnection();
    }
}