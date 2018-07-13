package com.yc.hadoop.hdfs.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce
 * @company 源辰信息
 * @author navy
 * 
 * Map的时候，我们先是拿到一行数据，然后分割，然后发送。
 * 发送的时候，我的格式是这样的：
 * 	Hello,1
 *  yc,1
 *  Hello,1
 *  world,1 
 *  ...
 *  但是，当我们Reduce的时候，此时Reduce拿到的键没有变，但值却是一个集合了。
 *  因为MapReduce的时候，它帮你做了一些事情，也就是说Map的数据不是直接发给Reduce的，而是先发给一个处理器处理后再给Reduce
 *  比如：拿到这个数据后
 *  Hello,1   先给你排序     Hello,1   然后呢将相同的key合并到一起     Hello, [1,1]
 *  yc,1                 Hello,1                             yc, [1]
 *  Hello,1              yc,1                                world, [1]
 *  world,1              world,1 
 *  所以，Reduce最终拿到的是一个集合，所以Reduce做统计的时候，只需要将集合中的值相加即可。
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * @param key 有mapreduce内部处理后的键
	 * @param values 将相同键，进行组合之后返回给我们的一个集合
	 * @param context 上下文
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		// 所以我们只需要变量值，然后累加就行了
		int total = 0;
		for (IntWritable iw : values) {
			total += iw.get();
		}
		
		context.write(key, new IntWritable(total));
	}
}
