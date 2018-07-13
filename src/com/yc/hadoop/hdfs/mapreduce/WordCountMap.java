package com.yc.hadoop.hdfs.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 第一、二个参数表示输入的key和value，从InputFormat传过来的，key默认是字符偏移量，value默认是一行
 * 第三、四个参数表示输出的key和value
 * 第一个参数：一般用Long类型，它是指字符的偏移量，一般不会使用
 * 第二个参数：表示一行的类型，对于文本而言，用String类型即可
 * 第三个参数：表示输出的键的类型，我们这里做字数统计，格式为<hello,1> <yc,1>，所以类型也是String
 * 第四个参数：表示输出的值的类型，所以我们这里用Integer就行了
 * 但是如果我们直接这样写public class WordCountMap extends Mapper<Long, String, String, Integer>，程序运行的时候会报错
 * 因为此时我们做的是分布式计算，每台服务器上运行一部分，然后进行整合。即，我们需要跨进程传递数据。
 * 但是在java中如果需要跨进程传递数据的话，该数据必须要进行序列化。所以，在使用类型的时候，不能直接使用java的类型，
 * 而是要使用hadoop中包装后的序列化类型。对应关系如下：
 * boolean BooleanWritable
 * byte    ByteWritable
 * int     IntWritable
 * float   FloatWritable
 * long    LongWritable
 * double  DoubleWritable
 * String  Text
 * Map     MapWritable
 * Array   ArrayWritable
 * @company 源辰信息
 * @author navy
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	/**
	 * @param key 是从源文件中传递过来的偏移量
	 * @param value 是从源文件中传递过来的一行数据
	 * @param context 上下文对象
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1. 拿到当前要处理的这一行数据，然后根据空格分割成数组
		String[] strs = value.toString().split(" ");

		// 2. 循环读取每个单词，发送到map中去，通过上下文对象Context发送
		for (String str : strs) {
			context.write(new Text(str), new IntWritable(1));
		}

		// 到此，map端搞定
	}
}
