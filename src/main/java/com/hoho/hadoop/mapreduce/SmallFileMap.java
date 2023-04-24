package com.hoho.hadoop.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.util.Arrays;

public class SmallFileMap {
    public static void main(String[] args) throws Exception {
        //生成MapFile文件
        write("/Users/tlc/Downloads/smallFile", "/mapFile");
        read("/mapFile");
    }

    /**
     * 生成MapFile文件
     *
     * @param inputDir  输入目录-windows目录
     * @param outputDir 输出目录-hdfs目录
     * @throws Exception
     */
    private static void write(String inputDir, String outputDir)
            throws Exception {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://hoho01:9000");
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        //删除输出目录
        fileSystem.delete(new Path(outputDir), true);
        //构造opts数组，有两个元素
         /*
         第一个是key类型
         第二个是value类型
         */
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)};
        //创建一个writer实例
        MapFile.Writer writer = new MapFile.Writer(conf, new Path(outputDir), opts);
        //指定要压缩的文件的目录
        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            Arrays.sort(files);
            for (File file : files) {
                //获取文件全部内容
                String content = FileUtils.readFileToString(file, "UTF-8");
                //文件名作为key
                Text key = new Text(file.getName());
                //文件内容作为value
                Text value = new Text(content);

                writer.append(key, value);
            }
        }
        writer.close();
    }

    /**
     * 读取MapFile文件
     *
     * @param inputDir MapFile文件路径
     * @throws Exception
     */
    private static void read(String inputDir) throws Exception {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://hoho01:9000");
        //创建阅读器
        MapFile.Reader reader = new MapFile.Reader(new Path(inputDir), conf);

        Text key = new Text();
        Text value = new Text();
        //循环读取数据
        while (reader.next(key, value)) {
            //输出文件名称
            System.out.print("文件名:" + key + ",");
            //输出文件的内容
            System.out.println("文件内容:" + value);
        }
        reader.close();
    }
}