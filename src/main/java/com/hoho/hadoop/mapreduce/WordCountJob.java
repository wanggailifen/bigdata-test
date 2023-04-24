package com.hoho.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * hadoop jar bigdata-test-1.0-SNAPSHOT.jar com.hoho.hadoop.mapreduce.WordCountJob /test /out
 */
public class WordCountJob {

    // <k1,v1> => <k2,v2>
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        /**
         * @param k1      每一行数据的行首偏移量
         * @param v1      每一行内容
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1,
                           Text v1,
                           Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            String[] words = v1.toString().split(" ");
            for (String word : words) {
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1);
                context.write(k2, v2);
            }
        }
    }

    // <k2,v2s> => <k3,v3>
    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text k2,
                              Iterable<LongWritable> v2s,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }
            Text k3 = k2;
            context.write(k3, new LongWritable(sum));
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            if (args.length != 2) {
                System.exit(100);
            }

            Job job = Job.getInstance(conf);
            // 设置map
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // 设置reduce
            job.setReducerClass(MyReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setJarByClass(WordCountJob.class);


            FileInputFormat.setInputPaths(job, new Path(args[0]));
            // 输出路径，必须不存在
            FileOutputFormat.setOutputPath(job, new Path(args[1]));



            job.waitForCompletion(true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
