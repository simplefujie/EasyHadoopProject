package com.fujie.mapreduce.combier;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Data input example: apple banana
 * Output of mapper is like: <apple,1> <banana,1>
 * 1. mapper will read file by row
 * 2. cut the line by blank space in order to get words
 * 3. output words in form of <key, value>
 * */
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
