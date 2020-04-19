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
