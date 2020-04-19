# 一、 基本类介绍

## 1. Mapper函数

### 1.1 函数要求

-  继承Hadoop的Mapper类

- 重写map方法

### 1.2 函数作用

- 读取原始数据，将输入数据以键值对的形式输出
- 使原始的无结构的数据变成有结构的数据

## 2. Reducer函数

### 2.1 函数要求

- 继承Hadoop的Reducer类
- 重写reduce方法

### 2.2 函数作用

- 从mapper中读取数据
- 对数据进行整理，作为最终的输出数据

## 3. Driver函数

### 3.1 函数要求

- 获取job对象
- 配置jar类，配置mapper类，reducer类
- 指定mapper输入输出类，指定最终结果的输入输出类
- 提交

### 3.2 函数作用

相当于配置了yarn的客户端，用于提交我们整个作业到yarn集群



# 二、 Word Count统计单词数

## 1. 实验说明

### 1.1 实验输入

```
atguigu atguigu
ss ss
cls cls
jiao
banzhang
xue
hadoop
```



### 1.2 期待输出

```
atguigu	2
banzhang	1
cls	
hadoop	1
jiao	1
ss	2
xue	1
```



### 1.3 实验说明

读取指定文件，统计每个单词出现的次数

## 2. 实验代码

### 2.1 Mapper类

1. 类介绍：

   从输入文件中一行一行的读取数据，然后以键值对的形式输出到reducer中。比如：读取到第一个atguigu的时候，会生成一个<atguigu,1>的键值对输出。在mapper阶段会并行的生成很多个键值对输出数据。

2. 类代码：

   ```java
   package com.fujie.mapreduce.wordcount;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   
   public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
   	
   	Text k=new Text();
   	IntWritable v=new IntWritable(1);
   
   	@Override
   	protected void map(LongWritable key, Text value, Context context)
   			throws IOException, InterruptedException {
   		
   		// get a line
   		String line=value.toString();
   		
   		// split by space
   		String[] words=line.split(" ");
   		
   		// output
   		for (String word : words) {
   			k.set(word);
   			context.write(k, v);
   		}
   	}
   }
   
   ```

### 2.2 Reducer类

1. 类介绍：

   Map阶段会生成很多键值对数据，这些键值对在经过一个shuffle阶段排序后，会输出到reducer。并且相同key的数据会被封装到一起。加入map输出的键值对信息如下所示：<atguigu,1>，<ss,1>，<atguigu,1>那么reducer会获取到的输入如下：<atguigu,[1,1]>，<ss,1>。所以reducer获取到的键值对形式为<key, values[…]>

   对每个key，recuder会取出它的values信息。从而进行后面的操作

2. 类代码：

   ```java
   package com.fujie.mapreduce.wordcount;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
   
   	int sum;
   	IntWritable v = new IntWritable();
   
   	@Override
   	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
   			throws IOException, InterruptedException {
   
   		// 1. sum the values
   		sum = 0;
   		for (IntWritable count : values) {
   			sum += count.get();
   		}
   
   		// 2. output the result
   		v.set(sum);
   		context.write(key, v);
   	}
   
   }
   
   ```

### 2.3 Driver类

1. 类介绍：

   就是正常的Driver类，获取job对象之后，这是相应的信息，然后提交

2. 类代码：

   ```java
   package com.fujie.mapreduce.wordcount;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
   /**
    * Driver will configure yarn
    * 
    * 1. Get job object
    * 
    * 2. Specify jar class, Mapper class and Reducer class (3)
    * 
    * 3. Specify Mapper input and output class and Final input and output class (2)
    * 
    * 4. Submit (1)
    */
   public class WordcountDriver {
   
   	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
   
           args = new String[] { "e:/input/wordcount", "e:/output/wordcount" };
   		// 1. Obtain configuration information and packaging tasks
   		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf);
   
   		// 2. Set jar loading path
   		job.setJarByClass(WordcountDriver.class);
   
   		// 3. set map and reduce class
   		job.setMapperClass(WordcountMapper.class);
   		job.setReducerClass(WordcountReducer.class);
   
   		// 4. set map output
   		job.setMapOutputKeyClass(Text.class);
   		job.setOutputValueClass(IntWritable.class);
   
   		// 5. set final k and v output type
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(IntWritable.class);
   
   		// 6. set input path and output path
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 7. submit
   		boolean result = job.waitForCompletion(true);
   		System.exit(result ? 0 : 1);
   	}
   
   }
   ```



# 三、 Flow Sum统计手机流量

## 1. 实验说明

### 1.1 实验输入

```
1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
2	13846544121	192.196.100.2			264	0	200
3 	13956435636	192.196.100.3			132	1512	200
4 	13966251146	192.168.100.1			240	0	404
5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
7 	13590439668	192.168.100.4			1116	954	200
8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
9 	13729199489	192.168.100.6			240	0	200
10 	13630577991	192.168.100.7	www.shouhu.com	6960	690	200
11 	15043685818	192.168.100.8	www.baidu.com	3659	3538	200
12 	15959002129	192.168.100.9	www.atguigu.com	1938	180	500
13 	13560439638	192.168.100.10			918	4938	200
14 	13470253144	192.168.100.11			180	180	200
15 	13682846555	192.168.100.12	www.qq.com	1938	2910	200
16 	13992314666	192.168.100.13	www.gaga.com	3008	3720	200
17 	13509468723	192.168.100.14	www.qinghua.com	7335	110349	404
18 	18390173782	192.168.100.15	www.sogou.com	9531	2412	200
19 	13975057813	192.168.100.16	www.baidu.com	11058	48243	200
20 	13768778790	192.168.100.17			120	120	200
21 	13568436656	192.168.100.18	www.alibaba.com	2481	24681	200
22 	13568436656	192.168.100.19			1116	954	200
```



### 1.2 期待输出

```

```



### 1.3 实验说明

读取手机的流量信息，然后输出手机的上行流量、下行流量和总流量

编写FlowBean类，实现手机流量的序列化

## 2. 实验代码

### 2.1 FlowBean类

1. 类说明：

   - 编写FlowBean类主要使为了实现序列化。序列化使为了使我们的自定义数据类型可以自由的传输，比如在<key, value>中传输
   - 实现Writable接口
   - 提供空参函数供反序列化时使用
   - 重写序列化方法和反序列化方法，发序列化的顺序必须和序列化的一致
   - 重写toString方法以把结果显示在文件中
   - 实现comparable接口，来使FlowBean可以放到<key, value>中传播。因为我们必须为shuffle提供比较的依据

2. 类代码：

   ```java
   package com.fujie.mapreduce.flowsum;
   
   import java.io.DataInput;
   import java.io.DataOutput;
   import java.io.IOException;
   
   import org.apache.hadoop.io.Writable;
   
   // 1. Implement the writable interface
   public class FlowBean implements Writable {
   
   	private long upFlow;
   	private long downFlow;
   	private long sumFlow;
   
   	// When deserializing, you need to call the empty parameter constructor, so you
   	// must have
   	public FlowBean() {
   		super();
   	}
   
   	public FlowBean(long upFlow, long downFlow) {
   		super();
   		this.upFlow = upFlow;
   		this.downFlow = downFlow;
   		this.sumFlow = upFlow + downFlow;
   	}
   
   	// Write serialization method
   	public void write(DataOutput out) throws IOException {
   		out.writeLong(upFlow);
   		out.writeLong(downFlow);
   		out.writeLong(sumFlow);
   	}
   
   	// Deserialization method
   	// The read sequence of the deserialization method must be the same as the write
   	// sequence of the write serialization method
   	public void readFields(DataInput in) throws IOException {
   		this.upFlow = in.readLong();
   		this.downFlow = in.readLong();
   		this.sumFlow = in.readLong();
   	}
   
   	// Write toString method to facilitate subsequent printing to text
   	@Override
   	public String toString() {
   		return upFlow + "\t" + downFlow + "\t" + sumFlow;
   	}
   
   	// set and get methods
   	public long getUpFlow() {
   		return upFlow;
   	}
   
   	public void setUpFlow(long upFlow) {
   		this.upFlow = upFlow;
   	}
   
   	public long getDownFlow() {
   		return downFlow;
   	}
   
   	public void setDownFlow(long downFlow) {
   		this.downFlow = downFlow;
   	}
   
   	public long getSumFlow() {
   		return sumFlow;
   	}
   
   	public void setSumFlow(long sumFlow) {
   		this.sumFlow = sumFlow;
   	}
   
   	public void set(long upFlow2, long downFlow2) {
   		this.upFlow = upFlow2;
   		this.downFlow = downFlow2;
   		this.sumFlow = upFlow2 + downFlow2;
   	}
   }
   ```

### 2.2 Mapper类

1. 类代码：

### 2.3 Reducer类

### 2.4 Driver类







