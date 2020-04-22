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



# 二、 WordCount：统计单词数

## 1. 实验介绍

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



# 三、 FlowSum：统计手机流量

## 1. 实验介绍

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
13470253144	180	180	360
13509468723	7335	110349	117684
13560439638	918	4938	5856
13568436656	3597	25635	29232
13590439668	1116	954	2070
13630577991	6960	690	7650
13682846555	1938	2910	4848
13729199489	240	0	240
13736230513	2481	24681	27162
13768778790	120	120	240
13846544121	264	0	264
13956435636	132	1512	1644
13966251146	240	0	240
13975057813	11058	48243	59301
13992314666	3008	3720	6728
15043685818	3659	3538	7197
15910133277	3156	2936	6092
15959002129	1938	180	2118
18271575951	1527	2106	3633
18390173782	9531	2412	11943
84188413	4116	1432	5548
```



### 1.3 实验说明

读取手机的流量信息，然后输出手机的上行流量、下行流量和总流量

编写FlowBean类，实现手机流量的序列化：使用Hadoop方式的序列化而不是Java的序列化，有很多的好处。

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

1. 类说明：
   
   从文件里读取数据，然后封装到FowBean对象中，传递到Reducer中
   
2. 类代码：

   ```java
   package com.fujie.mapreduce.flowsum;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   
   public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
   
   	// 1 13736230513 192.196.100.1 www.atguigu.com 2481 24681 200
   	FlowBean v = new FlowBean();
   	Text k = new Text();
   
   	@Override
   	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   
   		// 1. get a row
   		String line = value.toString();
   
   		// 2. cut field
   		String[] fields = line.split("\t");
   
   		// 3. package object,get phone number
   		String phoneNum = fields[1];
   
   		// 4. get upFlow and downFlow
   		long upFlow = Long.parseLong(fields[fields.length - 3]);
   		long downFlow = Long.parseLong(fields[fields.length - 2]);
   		k.set(phoneNum);
   		v.set(upFlow, downFlow);
   
   		// 5. write out
   		context.write(k, v);
   	}
   }
   
   ```

   

### 2.3 Reducer类

1. 类说明：

   从Mapper中读取FlowBean，然后汇总上行流量和下行流量

2. 类代码：

   ```java
   package com.fujie.mapreduce.flowsum;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
   	@Override
   	protected void reduce(Text key, Iterable<FlowBean> values, Context content)
   			throws IOException, InterruptedException {
   		long sum_upFlow = 0;
   		long sum_downFlow = 0;
   
   		// 1. Traverse the used beans and accumulate the upstream traffic and downstream
   		// traffic separately
   		for (FlowBean flowBean : values) {
   			sum_upFlow += flowBean.getUpFlow();
   			sum_downFlow += flowBean.getDownFlow();
   		}
   
   		// 2. Package object
   		FlowBean resutlBean = new FlowBean(sum_upFlow, sum_downFlow);
   
   		// 3. Write out
   		content.write(key, resutlBean);
   	}
   }
   
   ```

   

### 2.4 Driver类

1. 类说明：就是正常的Driver类，获取job对象之后，这是相应的信息，然后提交

2. 类代码：

   ```java
   package com.fujie.mapreduce.flowsum;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
   public class FlowsumDriver {
   
   	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
   		// Set input path and output path
   		args = new String[] { "e:/input/flowSum", "e:/output/flowSum" };
   
   		// 1. Get configuration information, or job object instance
   		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf);
   
   		// 2. Specify the local path where the jar package of this program is located
   		job.setJarByClass(FlowsumDriver.class);
   
   		// 3. Specify the mapper class and the reducer class
   		job.setMapperClass(FlowCountMapper.class);
   		job.setReducerClass(FlowCountReducer.class);
   
   		// 4. Specify map output key type and value type
   		job.setMapOutputKeyClass(Text.class);
   		job.setMapOutputValueClass(FlowBean.class);
   
   		// 5. Specify final output key type and value type
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(FlowBean.class);
   
   		// 6. Specify the directory where the original input file of the job is located
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 7. Submit
   		Boolean result = job.waitForCompletion(true);
   		System.exit(result ? 0 : 1);
   	}
   }
   ```



# 四、 InputFormat：并行度决定机制

CombineTextInputFormat机制：将原本划分为一个单独切片的小文件汇总起来为一个大的文件，这样可以避免小文件过多造成的性能浪费。

生成切片的过程：虚拟存储过程+切片过程

FileInputFormat实现类：

1. TextInputFormat：它使默认的FileInputFormat实现类，它按行读取每行消息，key值是每行起始字节的偏移量，value值是这行的内容
2. KeyValueTextInputFormat：每行均为一条记录，key值和value值由分隔符分隔开
3. NLineInputFormat：每N行划分为一个mapper进程
4. CombineTextInputFormat：



## 1. KeyValueInputFormat案例

### 1.1 实验介绍

#### 1.1.1 实验输入

```
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang
```

#### 1.1.2 期待输出

```
banzhang	2
xihuan	2
```

#### 1.1.3 实验说明

统计输入文件中每一行的第一个单词相同的行数。我们指定空格为分隔符，这样每行第一个单词就作为key。

### 1.2 实验代码

#### 1.2.1 Mapper类

1. 类说明：我们指定value为1，这样每次读取到同样的单词，就可以+1

2. 类代码：

   ```java
   package com.fujie.mapreduce.KeyValueTextInputFormat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   
   public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {
   
   	// 1 设置value
   	LongWritable v = new LongWritable(1);
   
   	@Override
   	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
   
   		// banzhang ni hao
   
   		// 2 写出
   		context.write(key, v);
   	}
   }
   ```

   

#### 1.2.2 Reducer类

1. 类说明：对Mapper传来的数据进行汇总+1即可

2. 类代码：

   ```java
   package com.fujie.mapreduce.KeyValueTextInputFormat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
   
   	LongWritable v = new LongWritable();
   
   	@Override
   	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
   			throws IOException, InterruptedException {
   
   		long sum = 0L;
   
   		// 1 汇总统计
   		for (LongWritable value : values) {
   			sum += value.get();
   		}
   
   		v.set(sum);
   
   		// 2 输出
   		context.write(key, v);
   	}
   }
   ```

   

#### 1.2.3 Driver类

1. 类说明：

   - 指定分隔符为空格：conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ")
   - 使用KeyValueTextInputFormat处理：job.setInputFormatClass(KeyValueTextInputFormat.class);

2. 类代码：

   ```java
   package com.fujie.mapreduce.KeyValueTextInputFormat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
   import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
   public class KVTextDriver {
   
   	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
   
   		args = new String[] { "e:/input/kvtInput", "e:/output/kvtOutput" };
   		Configuration conf = new Configuration();
   		// 设置切割符
   		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
   		// 1 获取job对象
   		Job job = Job.getInstance(conf);
   
   		// 2 设置jar包位置，关联mapper和reducer
   		job.setJarByClass(KVTextDriver.class);
   		job.setMapperClass(KVTextMapper.class);
   		job.setReducerClass(KVTextReducer.class);
   
   		// 3 设置map输出kv类型
   		job.setMapOutputKeyClass(Text.class);
   		job.setMapOutputValueClass(LongWritable.class);
   
   		// 4 设置最终输出kv类型
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(LongWritable.class);
   
   		// 5 设置输入输出数据路径
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   
   		// 设置输入格式，※
   		job.setInputFormatClass(KeyValueTextInputFormat.class);
   
   		// 6 设置输出数据路径
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 7 提交job
   		job.waitForCompletion(true);
   	}
   }
   ```

   

## 2. NLineInputFormat案例

### 2.1 实验介绍

#### 2.1.1 实验输入

```
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang banzhang ni hao
xihuan hadoop banzhang
```



#### 2.1.2 期待输出

```
banzhang	12
hadoop	6
hao	6
ni	6
xihuan	6
```

期待在Eclipse的控制台中看到：**Number of splits:4**

#### 2.1.3 实验说明

对每个单词进行个数统计，要求根据每个输入文件的行数来规定输出多少个切片。此案例要求每三行放入一个切片中。

### 2.2 实验代码

#### 2.2.1 Mapper类

1. 类说明：对每个单词输出为<key, value>到mapper中

2. 类代码：

   ```java
   package com.fujie.mapreduce.nline;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   
   /**
    * read file by rows, output like <banzhang,1> <ni,1> <hao,1>
    */
   public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
   
   //	banzhang ni hao
   //	xihuan hadoop banzhang
   //	banzhang ni hao
   	private Text k = new Text();
   	private LongWritable v = new LongWritable(1);
   
   	@Override
   	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   		// 1. Get a row
   		String line = value.toString();
   
   		// 2. Split
   		String[] splited = line.split(" ");
   
   		// 3. Write out
   		for (int i = 0; i < splited.length; i++) {
   			k.set(splited[i]);
   			context.write(k, v);
   		}
   	}
   }
   ```

   

#### 2.2.2 Reducer类

1. 类说明：对mapper传来的值进行汇总+1

2. 类代码：

   ```java
   package com.fujie.mapreduce.nline;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
   
   	LongWritable v = new LongWritable();
   
   	@Override
   	protected void reduce(Text key, Iterable<LongWritable> values, Context content)
   			throws IOException, InterruptedException {
   
   		long sum = 0l;
   
   		// 1. Generate all data
   		for (LongWritable value : values) {
   			sum += value.get();
   		}
   		v.set(sum);
   
   		// 2. Write out
   		content.write(key, v);
   	}
   }
   ```

   

#### 2.2.3 Driver类

1. 类说明：

   - 设置每个切片InputSplit中划分三条记录：NLineInputFormat.setNumLinesPerSplit(job, 3);
   - 使用NLineInputFormat来处理：job.setInputFormatClass(NLineInputFormat.class);  

2. 类代码：

   ```java
   package com.fujie.mapreduce.nline;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
   public class NLineDriver {
   
   	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
   
   		args = new String[] { "e:/input/nLineInput", "e:/output/nlineOutput" };
   
   		// 1. Get job object
   		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf);
   
   		// 7设置每个切片InputSplit中划分三条记录
   		NLineInputFormat.setNumLinesPerSplit(job, 3);
   
   		// 8使用NLineInputFormat处理记录数
   		job.setInputFormatClass(NLineInputFormat.class);
   
   		// 2设置jar包位置，关联mapper和reducer
   		job.setJarByClass(NLineDriver.class);
   		job.setMapperClass(NLineMapper.class);
   		job.setReducerClass(NLineReducer.class);
   
   		// 3设置map输出kv类型
   		job.setMapOutputKeyClass(Text.class);
   		job.setMapOutputValueClass(LongWritable.class);
   
   		// 4设置最终输出kv类型
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(LongWritable.class);
   
   		// 5设置输入输出数据路径
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 6提交job
   		job.waitForCompletion(true);
   
   	}
   }
   ```

   可以看到如下结果：

   ![image-20200419105719141.png](https://github.com/simplefujie/EasyHadoopProject/blob/master/studyNotes/02-mapreduce-experiments.assets/image-20200419105719141.png)

## 3. 自定义InputFormat案例

### 3.1 试验介绍

#### 3.1.1 实验输入

one.txt

```
yongpeng weidong weinan
sanfeng luozong xiaoming
```

two.txt

```
longlong fanfan
mazong kailun yuhang yixin
longlong fanfan
mazong kailun yuhang yixin
```

three.txt

```
shuaige changmo zhenqiang 
dongli lingu xuanxuan
```



#### 3.1.2 期待输出

```
SEQorg.apache.hadoop.io.Text"org.apache.hadoop.io.BytesWritable      ?8r鷑撾??60   ]   ('file:/e:/input/inputFormatInput/one.txt   1yongpeng weidong weinan
sanfeng luozong xiaoming   _   *)file:/e:/input/inputFormatInput/three.txt   1shuaige changmo zhenqiang 
dongli lingu xuanxuan   ?   ('file:/e:/input/inputFormatInput/two.txt   Xlonglong fanfan
mazong kailun yuhang yixin
longlong fanfan
mazong kailun yuhang yixin
```

期望输出文件格式：**part-r-00000**

#### 3.1.3 实验说明

- Q：为什么要自定义InputFormat

  A：在企业开发中，Hadoop自带的inputformat不能满足应用场景，需要使用自定义的

- Q：自定义InputFormat步骤：

  A：自定义步骤如下所示

  1. 自定义一个类继承FileInputFormat
  2. 改写RecordReader实现一次性读取一个完整文件为KV
  3. 输出时使用SequenceFileOutputFormat输出合并文件

将多个小文件合并成一个SequenceFile文件（SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式），SequenceFile里面存储着多个文件，存储的形式为文件路径+名称为key，文件内容为value。需求分析如下：

1. 自定义一个类继承：FileInputFormat
   - 重写isSplitable()方法，返回false表示不可切割
   - 重写createRecordReader()，创建自定义的RecordReader对象，并初始化
2. 改写RecordReader，实现一次读取一个完整的文件封装为KV
   - 采用了IO流一次读取一个文件输出到value中
   - 获取文件路径信息+名称，并设置key
3. 设置Driver
   - 设置输入的inputFormat类型：job.setInputFormatClass(WholeFileInputformat.class);
   - 设置输出的outputFormat类型：job.setOutputFormatClass(SequenceFileOutputFormat.class);



### 3.2 实验代码

#### 3.2.1 WholeRecordReader类

1. 类说明：每当读取一个文件的时候，以该文件的路径+名字作为Key，以文件内容作为value

2. 类代码：

   ```java
   package com.fujie.mapreduce.inputformat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.FSDataInputStream;
   import org.apache.hadoop.fs.FileSystem;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.BytesWritable;
   import org.apache.hadoop.io.IOUtils;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.InputSplit;
   import org.apache.hadoop.mapreduce.RecordReader;
   import org.apache.hadoop.mapreduce.TaskAttemptContext;
   import org.apache.hadoop.mapreduce.lib.input.FileSplit;
   
   public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
   
   	private Configuration configuration;
   	private FileSplit split;
   
   	private boolean isProgress = true;
   	private BytesWritable value = new BytesWritable();
   	private Text k = new Text();
   
   	@Override
   	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
   
   		this.split = (FileSplit) split;
   		configuration = context.getConfiguration();
   	}
   
   	@Override
   	public boolean nextKeyValue() throws IOException, InterruptedException {
   		if (isProgress) {
   			// 1. Define buffer
   			byte[] contents = new byte[(int) split.getLength()];
   			FileSystem fs = null;
   			FSDataInputStream fis = null;
   
   			try {
   				// 2. Get file system
   				Path path = split.getPath();
   				fs = path.getFileSystem(configuration);
   
   				// 3. Read data
   				fis = fs.open(path);
   
   				// 4. Get file content
   				IOUtils.readFully(fis, contents, 0, contents.length);
   
   				// 5. Output file content
   				value.set(contents, 0, contents.length);
   
   				// 6. Get file path and file name
   				String name = split.getPath().toString();
   
   				// 7. Set output key
   				k.set(name);
   			} catch (Exception e) {
   				// TODO: handle exception
   			} finally {
   				IOUtils.closeStream(fis);
   			}
   			isProgress = false;
   			return true;
   		}
   		return false;
   	}
   
   	@Override
   	public Text getCurrentKey() throws IOException, InterruptedException {
   		return k;
   	}
   
   	@Override
   	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
   		return value;
   	}
   
   	@Override
   	public float getProgress() throws IOException, InterruptedException {
   		return 0;
   	}
   
   	@Override
   	public void close() throws IOException {
   	}
   
   }
   ```

   

#### 3.2.2 WholeFileInputFormat类

1. 类说明：初始化并且返回我们自定义的WholeRecordReader对象

2. 类代码：

   ```java
   package com.fujie.mapreduce.inputformat;
   
   import java.io.IOException;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.BytesWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.InputSplit;
   import org.apache.hadoop.mapreduce.JobContext;
   import org.apache.hadoop.mapreduce.RecordReader;
   import org.apache.hadoop.mapreduce.TaskAttemptContext;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   
   // 定义类继承FileInputFormat
   public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable> {
   
   	@Override
   	protected boolean isSplitable(JobContext context, Path filename) {
   		return false;
   	}
   
   	@Override
   	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
   			throws IOException, InterruptedException {
   
   		WholeRecordReader recordReader = new WholeRecordReader();
   		recordReader.initialize(split, context);
   
   		return recordReader;
   	}
   }
   ```

   

#### 3.2.3 SequenceFileMapper类

1. 类说明：以文件名最为Key，文件内容作为Value。提供给reducer

2. 类代码：

   ```java
   package com.fujie.mapreduce.inputformat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.BytesWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   
   public class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
   
   	@Override
   	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
   
   		context.write(key, value);
   	}
   }
   ```

   

#### 3.2.4 SequenceFileReducer类

1. 类说明：将key和value写出到最终输出

2. 类代码：

   ```java
   package com.fujie.mapreduce.inputformat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.BytesWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
   
   	@Override
   	protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
   			throws IOException, InterruptedException {
   
   		context.write(key, values.iterator().next());
   	}
   }
   ```

   

#### 3.2.5 SequeceFileDriver类

1. 类说明：

   - 设置输入的inputFormat类型：job.setInputFormatClass(WholeFileInputformat.class);
   - 设置输出的outputFormat类型：job.setOutputFormatClass(SequenceFileOutputFormat.class);

2. 类代码：

   ```java
   package com.fujie.mapreduce.inputformat;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.BytesWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
   
   public class SequenceFileDriver {
   
   	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
   
   		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
   		args = new String[] { "e:/input/inputFormatInput", "e:/output/inputFormatOutput" };
   
   		// 1 获取job对象
   		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf);
   
   		// 2 设置jar包存储位置、关联自定义的mapper和reducer
   		job.setJarByClass(SequenceFileDriver.class);
   		job.setMapperClass(SequenceFileMapper.class);
   		job.setReducerClass(SequenceFileReducer.class);
   
   		// 7设置输入的inputFormat
   		job.setInputFormatClass(WholeFileInputformat.class);
   
   		// 8设置输出的outputFormat
   		job.setOutputFormatClass(SequenceFileOutputFormat.class);
   
   // 3 设置map输出端的kv类型
   		job.setMapOutputKeyClass(Text.class);
   		job.setMapOutputValueClass(BytesWritable.class);
   
   		// 4 设置最终输出端的kv类型
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(BytesWritable.class);
   
   		// 5 设置输入输出路径
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 6 提交job
   		boolean result = job.waitForCompletion(true);
   		System.exit(result ? 0 : 1);
   	}
   }
   ```



# 五、 Partition分区：将结果输出到多个文件中

## 1. 实验介绍

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
13470253144	180	180	360
13509468723	7335	110349	117684
13560439638	918	4938	5856
13568436656	3597	25635	29232
13590439668	1116	954	2070
13630577991	6960	690	7650
13682846555	1938	2910	4848
13729199489	240	0	240
13736230513	2481	24681	27162
13768778790	120	120	240
13846544121	264	0	264
13956435636	132	1512	1644
13966251146	240	0	240
13975057813	11058	48243	59301
13992314666	3008	3720	6728
15043685818	3659	3538	7197
15910133277	3156	2936	6092
15959002129	1938	180	2118
18271575951	1527	2106	3633
18390173782	9531	2412	11943
84188413	4116	1432	5548
```

应该有5个输出文件：

![image-20200419155255436](.\02-mapreduce-experiments.assets\image-20200419155255436.png)

### 1.3 实验说明

读取手机的流量信息，然后输出手机的上行流量、下行流量和总流量

手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。

## 2. 实验代码

### 2.1 ProvincePartitioner类

1. 类说明：我们在FlowSum实验的基础上添加了ProvincePartitioner类，用来区分不同的手机号。继承Partitioner类，重写getPartition方法

2. 类代码：

   ```java
   package com.fujie.mapreduce.partition;
   
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Partitioner;
   
   public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
   
   	@Override
   	public int getPartition(Text key, FlowBean value, int numPartitions) {
   
   		// 1. Get first 3 numbers of phone number
   		String preNum = key.toString().substring(0, 3);
   
   		int partition = 4;
   		// 2. Verify province
   		if ("136".equals(preNum)) {
   			partition = 0;
   		} else if ("137".equals(preNum)) {
   			partition = 1;
   		} else if ("138".equals(preNum)) {
   			partition = 2;
   		} else if ("139".equals(preNum)) {
   			partition = 3;
   		}
   		return partition;
   	}
   }
   ```

### 2.2 FlowsumDriver类

1. 类说明：增加自定义数据分区设置和ReduceTask设置

2. 类代码：

   ```java
   package com.fujie.mapreduce.partition;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
   public class FlowsumDriver {
   
   	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
   		// Set input path and output path
   		args = new String[] { "e:/input/partition", "e:/output/partition" };
   
   		// 1. Get configuration information, or job object instance
   		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf);
   
   		// 2. Specify the local path where the jar package of this program is located
   		job.setJarByClass(FlowsumDriver.class);
   
   		// 3. Specify the mapper class and the reducer class
   		job.setMapperClass(FlowCountMapper.class);
   		job.setReducerClass(FlowCountReducer.class);
   
   		// 4. Specify map output key type and value type
   		job.setMapOutputKeyClass(Text.class);
   		job.setMapOutputValueClass(FlowBean.class);
   
   		// 5. Specify final output key type and value type
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(FlowBean.class);
   
   		// 8 指定自定义数据分区
   		job.setPartitionerClass(ProvincePartitioner.class);
   
   		// 9 同时指定相应数量的reduce task
   		job.setNumReduceTasks(5);
   
   		// 6. Specify the directory where the original input file of the job is located
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 7. Submit
   		Boolean result = job.waitForCompletion(true);
   		System.exit(result ? 0 : 1);
   	}
   }
   
   ```

# 六、 排序

1. map的排序过程：

   - 默认排序是按照字典顺序排序，且实现该排序的方法是快速排序
   - 对于MapTask，它会将处理的结果暂时放到环形缓冲区中，当环形缓冲区使用率达到一定阈值后，再对缓冲区中的数据进行一次快速排序，并将这些有序数据溢写到磁盘上，而当数据处理完毕后，它会对磁盘上所有文件进行归并排序。

2. reduce排序过程：

   对于ReduceTask，它从每个MapTask上远程拷贝相应的数据文件，如果文件大小超过一定阈值，则溢写磁盘上，否则存储在内存中。如果磁盘上文件数目达到一定阈值，则进行一次归并排序以生成一个更大文件；如果内存中文件大小或者数目超过一定阈值，则进行一次合并后将数据溢写到磁盘上。当所有数据拷贝完毕后，ReduceTask统一对内存和磁盘上的所有数据进行一次归并排序。



## 1. FlowCountSort排序案例

### 1.1 实验介绍

#### 1.1.1 实验输入

```
13470253144	180	180	360
13509468723	7335	110349	117684
13560439638	918	4938	5856
13568436656	3597	25635	29232
13590439668	1116	954	2070
13630577991	6960	690	7650
13682846555	1938	2910	4848
13729199489	240	0	240
13736230513	2481	24681	27162
13768778790	120	120	240
13846544121	264	0	264
13956435636	132	1512	1644
13966251146	240	0	240
13975057813	11058	48243	59301
13992314666	3008	3720	6728
15043685818	3659	3538	7197
15910133277	3156	2936	6092
15959002129	1938	180	2118
18271575951	1527	2106	3633
18390173782	9531	2412	11943
84188413	4116	1432	5548
```



#### 1.1.2 期待输出

```
c
```



#### 1.1.3 实验说明

根据案例2.3 Flowsum产生的结果再次对总流量进行排序

### 1.2 实验代码

#### 2.1.1 FlowBean类

1. 类介绍：在原始的基础上，增加了比较部分compareTo

2. 类代码：

   ```java
   package com.fujie.mapreduce.sort;
   
   import java.io.DataInput;
   import java.io.DataOutput;
   import java.io.IOException;
   
   import org.apache.hadoop.io.WritableComparable;
   
   // 1. Implement the writable interface
   public class FlowBean implements WritableComparable<FlowBean> {
   
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
   
   	// Add compare function
   	public int compareTo(FlowBean bean) {
   
   		int result;
   		// According to the total flow size, in reverse order
   		if (sumFlow > bean.getSumFlow()) {
   			result = -1;
   		} else if (sumFlow < bean.getSumFlow()) {
   			result = 1;
   		} else {
   			result = 0;
   		}
   
   		return result;
   
   	}
   }
   ```

#### 1.2.2 FlowCountSortMapper类

1. 类介绍：

   输出key为Bean，因为排序过程是也key作为排序的，所以要把Bean作为key

   输出value为phoneNumber

2. 类代码：

   ```java
   package com.fujie.mapreduce.sort;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Mapper;
   
   public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
   
   	FlowBean bean = new FlowBean();
   	Text v = new Text();
   
   	@Override
   	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   
   		// 1 获取一行
   		String line = value.toString();
   
   		// 2 截取
   		String[] fields = line.split("\t");
   
   		// 3 封装对象
   		String phoneNbr = fields[0];
   		long upFlow = Long.parseLong(fields[1]);
   		long downFlow = Long.parseLong(fields[2]);
   
   		bean.set(upFlow, downFlow);
   		v.set(phoneNbr);
   
   		// 4 输出
   		context.write(bean, v);
   	}
   }
   ```

   

#### 1.2.3 FlowCountSortReducer类

1. 类介绍：

   输出结果key为手机号

   输出结果value为Bean

2. 类代码：

   ```java
   package com.fujie.mapreduce.sort;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
   
   	@Override
   	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
   			throws IOException, InterruptedException {
   
   		// 循环输出，避免总流量相同情况
   		for (Text text : values) {
   			context.write(text, key);
   		}
   	}
   }
   ```



## 2. WordcountCombiner案例：区内排序

### 2.1 实验介绍

#### 2.1.1 实验输入

```
13470253144	180	180	360
13509468723	7335	110349	117684
13560439638	918	4938	5856
13568436656	3597	25635	29232
13590439668	1116	954	2070
13630577991	6960	690	7650
13682846555	1938	2910	4848
13729199489	240	0	240
13736230513	2481	24681	27162
13768778790	120	120	240
13846544121	264	0	264
13956435636	132	1512	1644
13966251146	240	0	240
13975057813	11058	48243	59301
13992314666	3008	3720	6728
15043685818	3659	3538	7197
15910133277	3156	2936	6092
15959002129	1938	180	2118
18271575951	1527	2106	3633
18390173782	9531	2412	11943
84188413	4116	1432	5548
```



#### 2.1.2 期待输出

```
13470253144	180	180	360
13509468723	7335	110349	117684
13560439638	918	4938	5856
13568436656	3597	25635	29232
13590439668	1116	954	2070
13630577991	6960	690	7650
13682846555	1938	2910	4848
13729199489	240	0	240
13736230513	2481	24681	27162
13768778790	120	120	240
13846544121	264	0	264
13956435636	132	1512	1644
13966251146	240	0	240
13975057813	11058	48243	59301
13992314666	3008	3720	6728
15043685818	3659	3538	7197
15910133277	3156	2936	6092
15959002129	1938	180	2118
18271575951	1527	2106	3633
18390173782	9531	2412	11943
84188413	4116	1432	5548
```

期待看到多个输出文件，每个文件内都是有序的

#### 2.1.3 实验说明

在上一个实验的基础上，添加了自定义分区类

在Driver中设置了自定义分区类和Reducetask的个数

### 2.2 实验代码

#### 2.2.1 ProvincePartitioner类

1. 类说明：extends Partitioner<FlowBean, Text>

2. 类代码：

   ```java
   package com.fujie.mapreduce.partitionSort;
   
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Partitioner;
   
   public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
   
   	@Override
   	public int getPartition(FlowBean key, Text value, int numPartitions) {
   
   		// 1 获取手机号码前三位
   		String preNum = value.toString().substring(0, 3);
   
   		int partition = 4;
   
   		// 2 根据手机号归属地设置分区
   		if ("136".equals(preNum)) {
   			partition = 0;
   		} else if ("137".equals(preNum)) {
   			partition = 1;
   		} else if ("138".equals(preNum)) {
   			partition = 2;
   		} else if ("139".equals(preNum)) {
   			partition = 3;
   		}
   
   		return partition;
   	}
   }
   ```

   

#### 2.2.2 FlowCountSortDriver类

1. 类说明：设置了自定义分区类和Reducetask的个数

2. 类代码：

   ```java
   package com.fujie.mapreduce.partitionSort;
   
   import java.io.IOException;
   
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
   public class FlowCountSortDriver {
   
   	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
   
   		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
   		args = new String[] { "e:/output/flowSum", "e:/output/partitionSort" };
   
   		// 1 获取配置信息，或者job对象实例
   		Configuration configuration = new Configuration();
   		Job job = Job.getInstance(configuration);
   
   		// 2 指定本程序的jar包所在的本地路径
   		job.setJarByClass(FlowCountSortDriver.class);
   
   		// 3 指定本业务job要使用的mapper/Reducer业务类
   		job.setMapperClass(FlowCountSortMapper.class);
   		job.setReducerClass(FlowCountSortReducer.class);
   
   		// 4 指定mapper输出数据的kv类型
   		job.setMapOutputKeyClass(FlowBean.class);
   		job.setMapOutputValueClass(Text.class);
   
   		// 5 指定最终输出的数据的kv类型
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(FlowBean.class);
   
   		// 加载自定义分区类
   		job.setPartitionerClass(ProvincePartitioner.class);
   
   		// 设置Reducetask个数
   		job.setNumReduceTasks(5);
   
   		// 6 指定job的输入原始文件所在目录
   		FileInputFormat.setInputPaths(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   
   		// 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
   		boolean result = job.waitForCompletion(true);
   		System.exit(result ? 0 : 1);
   	}
   }
   ```



# 七、 Conbiner合并：将map输出进行合并，从而减小网络传输量

## 

## 1. 试验介绍

### 1.1 实验输入

```
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang
```



### 1.2 期待输出

```
banzhang	4
hadoop	2
hao	2
ni	2
xihuan	2
```

可以看到combiner发生了作用，本来map的输出由12个变成了5个

![image-20200421161906124](.\02-mapreduce-experiments.assets\image-20200421161906124.png)

### 1.3 实验说明

增加一个WordcountCombiner类继承Reducer

## 2. 实验代码

### 2.1 WordcountCombiner类

1. 类说明：这个类和Reducer类很像，就是做了汇总的工作

2. 类代码：

   ```java
   package com.fujie.mapreduce.combier;
   
   import java.io.IOException;
   
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   
   public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
   
   	IntWritable v = new IntWritable();
   
   	@Override
   	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
   			throws IOException, InterruptedException {
   		// 1. Summarize
   		int sum = 0;
   		for (IntWritable value : values) {
   			sum += value.get();
   		}
   		v.set(sum);
   
   		// 2. Write out
   		context.write(key, v);
   	}
   }
   
   ```

### 2.2 WordcountDriver类

1. 类说明：指定了CombinerClass，直接指定CombinerClass为Reducer类也是可以的

2. 类代码：

   ```java
   package com.fujie.mapreduce.combier;
   
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
   
   		args = new String[] { "e:/input/combiner", "e:/output/combiner" };
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
   
   		// 8. set combiner class
   		job.setCombinerClass(WordcountCombiner.class);
   
   		// 7. submit
   		boolean result = job.waitForCompletion(true);
   		System.exit(result ? 0 : 1);
   	}
   
   }
   ```

# 八、 GroupingComparator分组（辅助排序）

## 1. 试验介绍

### 1.1 实验输入

```
0000001	Pdt_01	222.8
0000002	Pdt_05	722.4
0000001	Pdt_02	33.8
0000003	Pdt_06	232.8
0000003	Pdt_02	33.8
0000002	Pdt_03	522.8
0000002	Pdt_04	122.4
```



### 1.2 期待输出

```
1	222.8
2	722.4
3	232.8
```



### 1.3 实验说明

（1）利用“订单id和成交金额”作为key，可以将Map阶段读取到的所有订单数据按照id升序排序，如果id相同再按照金额降序排序，发送到Reduce。

（2）在Reduce端利用groupingComparator将订单id相同的kv聚合成组，然后取第一个即是该订单中最贵商品，如图4-18所示。

## 2. 实验代码

### 2.1 OrderBean类

1. 类说明：定义订单信息

2. 类代码：

   ```java
   package com.fujie.mapreduce.order;
   
import java.io.DataInput;
   import java.io.DataOutput;
   import java.io.IOException;
   
   import org.apache.hadoop.io.WritableComparable;
   
   public class OrderBean implements WritableComparable<OrderBean> {
   
   	private int order_id; // 订单id号
   	private double price; // 价格
   
   	public OrderBean(int order_id, double price) {
   		super();
   		this.order_id = order_id;
   		this.price = price;
   	}
   
   	public OrderBean() {
   		super();
   	}
   
   	public void write(DataOutput out) throws IOException {
   		out.writeInt(order_id);
   		out.writeDouble(price);
   	}
   
   	public void readFields(DataInput in) throws IOException {
   		order_id = in.readInt();
   		price = in.readDouble();
   	}
   
   	// sort
   	public int compareTo(OrderBean o) {
   		int result;
   
   		if (order_id > o.getOrder_id()) {
   			result = 1;
   		} else if (order_id < o.getOrder_id()) {
   			result = -1;
   		} else {
   			// Secondary sort
   			result = price > o.getPrice() ? -1 : 1;
   		}
   
   		return result;
   
   	}
   
   	@Override
   	public String toString() {
   		return order_id + "\t" + price;
   	}
   
   	public int getOrder_id() {
   		return order_id;
   	}
   
   	public void setOrder_id(int order_id) {
   		this.order_id = order_id;
   	}
   
   	public double getPrice() {
   		return price;
   	}
   
   	public void setPrice(double price) {
   		this.price = price;
   	}
   
   }
   ```
   

### 2.2 OrderGroupingComparator类

1. 类说明：定义了排序时比较的方法

2. 类代码：

   ```java
   package com.fujie.mapreduce.order;
   
   import org.apache.hadoop.io.WritableComparable;
   import org.apache.hadoop.io.WritableComparator;
   
   public class OrderGroupingComparator extends WritableComparator {
   
   	protected OrderGroupingComparator() {
   		super(OrderBean.class, true);
   	}
   
   	@Override
   	public int compare(WritableComparable a, WritableComparable b) {
   
   		OrderBean aBean = (OrderBean) a;
   		OrderBean bBean = (OrderBean) b;
   
   		int result;
   		if (aBean.getOrder_id() > bBean.getOrder_id()) {
   			result = 1;
   		} else if (aBean.getOrder_id() < bBean.getOrder_id()) {
   			result = -1;
   		} else {
   			result = 0;
   		}
   
   		return result;
   	}
   }
   ```

   

# 九、 OutputFormat实验









