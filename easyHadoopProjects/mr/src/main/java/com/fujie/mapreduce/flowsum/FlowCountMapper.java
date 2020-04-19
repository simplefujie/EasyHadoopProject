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
