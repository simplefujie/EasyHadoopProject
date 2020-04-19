package com.fujie.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer will get input from mapper, objects with the same key will be submit
 * together by mapper.
 * 
 * For example, if mapper submit <apple,1> <banana,1> <apple,1>.
 * 
 * Reducer will get <apple,[1,1]> <banana,[1]>
 * 
 * Reducer will convert <apple,[1,1]> into <apple,2>
 */
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
