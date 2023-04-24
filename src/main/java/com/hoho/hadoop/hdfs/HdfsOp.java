package com.hoho.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsOp {
    public static void main(String[] args) throws Exception {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://hoho01:9000");
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);

        put(fileSystem);


    }

    private static void put(FileSystem fileSystem) throws IOException {
        //获取HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/Hello1.txt"));
        //获取本地文件的输入流
        FileInputStream fis = new FileInputStream("/Users/tlc/Downloads/Hello.txt");

        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    /**
     * 下载文件
     * @param fileSystem
     * @throws IOException
     */
    private static void get(FileSystem fileSystem) throws IOException{
        //获取HDFS文件系统的输入流
        FSDataInputStream fis = fileSystem.open(new Path("/Hello.txt"));
        //获取本地文件的输出流
        FileOutputStream fos = new FileOutputStream("/Users/tlc/Downloads/Hello2.txt");
        //下载文件
        IOUtils.copyBytes(fis,fos,1024,true);
    }

    /**
     * 删除文件
     * @param fileSystem
     * @throws IOException
     */
    private static void delete(FileSystem fileSystem) throws IOException{
        //删除文件，目录也可以删除
        //如果要递归删除目录，则第二个参数需要设置为true
        //如果是删除文件或者空目录，第二个参数会被忽略
        boolean flag = fileSystem.delete(new Path("/Hello.txt"),true);
        if(flag){
            System.out.println("删除成功！");
        }else{
            System.out.println("删除失败！");
        }
    }

}
