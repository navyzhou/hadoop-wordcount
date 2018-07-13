简单的MapReduce程序的实习步骤：
1. 编程Mapper类
	继承Mapper类，实现其中的setup()、map()、cleanup()方法。如果是简单的实现，可以只实现map()方法
	
2. 编写Reducer类
	继承Reducer类，实现其中的setup()、map()、cleanup()方法。
	
3. 编写Driver类
	在main方法里定义运行作业，定义一个job，在这里控制obj如何运行等。
	
4. 将编写好的程序打成jar包，然后放到Hadoop服务器中
	使用hadoop jar <[项目名].jar> com.yc.*.<主类名> 即可