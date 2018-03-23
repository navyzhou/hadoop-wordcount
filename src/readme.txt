1、导包
	(1) hadoop-3.0.0\share\hadoop\mapreduce下的jar包
	(2) hadoop-3.0.0\share\hadoop\common下的jar包

1、map任务处理
	(1) 读取文件内容，解析成key、value对。每一个键值对调用一个map函数。
	(2) 在map函数中可以编写自己的逻辑，对输入的key、value处理，转换成新的key、value输出。
	(3) 对输出的key、value进行分区。
	(4) 对不同分区的数据，按照key进行排序、分组。相同key的value放到一个集合中。

2、reduce任务处理
	(1) 对多个map任务的输出，按照不同的分区，通过网络copy到不同的reduce节点。
	(2) 对多个map任务的输出进行合并、排序。写reduce函数自己的逻辑，对输入的key、reduce处理，转换成新的key、value输出。
	(3) 把reduce的输出保存到文件中。

	
Context 类是Mapper 类的内部抽象类，它实现了MapContext 接口MapContext 里面可以得到split的信息，这个接口实现了 TaskInputOutputContext 这个接口
TaskInputOutputContext 这个接口里面一些记录 getCurrentKey、getCurrentValue、nextKeyValue, getOutputCommitter
（这个是一个OutputCommitter的抽象类，这个提供了提交的一些操作方法和属性）的方法，这个接口实现了TaskAttemptContext这个接口
​TaskAttemptContext 这个接口保存了 task的一些信息，这个接口实现了JobContext和Progressable这个接口
​JobContext和Progressable这个2个接口，这2个接口保存了job的信息和程序运行过程的进展
	
