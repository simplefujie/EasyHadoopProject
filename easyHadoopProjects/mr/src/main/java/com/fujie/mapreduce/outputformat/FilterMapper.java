package com.fujie.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	// http://www.baidu.com
	// http://www.google.com
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Ð´³ö
		context.write(value, NullWritable.get());
	}
}
