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
