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
