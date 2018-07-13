package com.yc.hadoop.hdfs.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 作业类，主要的作用就是将Map和Reduce关联起来
 * @company 源辰信息
 * @author navy
 */
public class WordJob {
	public static void main(String[] args) {
		
		try {
			// 创建一个job
			Job job = Job.getInstance(new Configuration());
			job.setJobName("wordCountTest"); // 给这个job取一个名字
			job.setJarByClass(WordJob.class ); // 设置job类，即这个程序的入口类
			
			// 让job与map类和reduce关联
			job.setMapperClass(WordCountMap.class);
			job.setReducerClass(WordCountReduce.class);
			
			// 设置mapper的输出数据类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			// 指定要统计的数据源
			FileInputFormat.addInputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/yc.txt"));

			// 指定结果的输出路径
			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/wordcount"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
