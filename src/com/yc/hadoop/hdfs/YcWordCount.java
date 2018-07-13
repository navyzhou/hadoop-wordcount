package com.yc.hadoop.hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 单词统计
 * @company 源辰信息
 * @author navy
 * 导包:
 * 	hadoop-3.0.0\share\hadoop\mapreduce下的jar包
 * 	hadoop-3.0.0\share\hadoop\common下的jar包
 *  hadoop-3.0.0\share\hadoop\yarn下的jar包
 */
public class YcWordCount {
	
	/**
	 * 第一、二个参数表示输入map的key和value，从InputFormat传过来的，key默认是字符偏移量，value默认是一行
	 * 第三、四个表示输出的key和value
	 * @company 源辰信息
	 * @author navy
	 */
	public static class TokenizerMapper	extends Mapper<Object, Text, Text, IntWritable>{
		/*
		 * IntWritable是 Hadoop 中实现的用于封装 Java 数据类型的类,它的原型是public IntWritable(int value)和public IntWritable()两种。
		 * 所以new IntWritable(1)是创建了这个类的一个对象，而数值1这是参数。在Hadoop中它相当于java中Integer整型变量，为这个变量赋值为1.
		 */
		private final static IntWritable one = new IntWritable(1);
		
		/*
		 * 这类存储的文本使用标准UTF8编码。它提供了序列化、反序列化和比较文本的方法。 
		 * Text类使用整型来存储字符串编码中所需的字节数。即Text存储的是字节数，这个字节数用整型存储，并且使用零压缩格式进行序列化。
		 * 此外，它还提供了字符串遍历的方法，而无需将字节数组转换为字符串。
		 */
		private Text word = new Text();  // 输出的键 单词

		/*
		 * context它是mapper的一个内部类，简单的说顶级接口是为了在map或是reduce任务中跟踪task的状态，很自然的MapContext就是记录了map执行的上下文，
		 * 在mapper类中，这个context可以存储一些job、conf的信息，比如job运行时参数等，我们可以在map函数中处理这个信息，这也是hadoop中参数传递中一个很经典的例子，
		 * 同时context作为了map和reduce执行中各个函数的一个桥梁，这个设计和java web中的session对象、application对象很相似(non-Javadoc)
		 */
		// 处理经过  TextInputFormat  产生的  <k1,v1>，然后产生 <k2,v2>
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//它是一个很方便的字符串分解器，主要用来根据分隔符把字符串分割成标记（Token），然后按照请求返回各个标记。
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());  // 读取到的单词作为键值
				context.write(word,one); // 以单词,1的中间形式交给reduce处理
			}
		}
	}

	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		
		job.setJarByClass(YcWordCount.class);
		job.setCombinerClass(IntSumReducer.class);
		
		// 设置MapReduce类
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		// 设置输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 * 报错 java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z
		 * 请查看报错文档
		 */
		// FileInputFormat.addInputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/yc.txt"));
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/*"));
		
		// 查看hdfs上的文件和目录  hadoop fs -ls /
		// 删除目录 hadoop fs -rm -r /<目录名>
		// 查看统计信息  hadoop fs -cat wordcount/part-r-00000
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/wordcount"));
		boolean bl = job.waitForCompletion(true);
		if (bl) {
			System.out.println("统计完成...");
		} else {
			System.out.println("统计失败...");
		}
	}
}
