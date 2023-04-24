package com.hoho.hadoop.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;

public class SmallFileSeq {
    public static void main(String[] args) throws Exception {
        //生成SequenceFile文件
//        write("/Users/tlc/Downloads/smallFile", "/seqFile");
        //读取SequenceFile文件
        read("/seqFile");
    }

    /**
     * 生成SequenceFile文件
     *
     * @param inputDir   输入目录-windows目录
     * @param outputFile 输出文件-hdfs文件
     * @throws Exception
     */
    private static void write(String inputDir, String outputFile)
            throws Exception {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://hoho01:9000");
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        //删除输出文件
        fileSystem.delete(new Path(outputFile), true);
        //构造opts数组，有三个元素
         /*
         第一个是输出路径
         第二个是key类型
         第三个是value类型
         */
        SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                SequenceFile.Writer.file(new Path(outputFile)),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)};
        //创建一个writer实例
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, opts);
        //指定要压缩的文件的目录
        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
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

    private static void read(String inputFile) throws Exception {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS", "hdfs://hoho01:9000");
        //创建阅读器
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(inputFile)));
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