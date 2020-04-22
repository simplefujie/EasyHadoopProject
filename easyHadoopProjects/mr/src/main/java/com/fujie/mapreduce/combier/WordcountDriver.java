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
