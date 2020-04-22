package com.fujie.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	String name;
	Text k = new Text();
	IntWritable v = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// ��ȡ�ļ�����
		FileSplit split = (FileSplit) context.getInputSplit();

		name = split.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 ��ȡ1��
		String line = value.toString();

		// 2 �и�
		String[] fields = line.split(" ");

		for (String word : fields) {

			// 3 ƴ��
			k.set(word + "--" + name);
			v.set(1);

			// 4 д��
			context.write(k, v);
		}
	}
}
