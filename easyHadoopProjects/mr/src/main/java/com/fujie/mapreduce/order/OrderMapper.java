package com.fujie.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

	// 0000001 Pdt_01 222.8

	OrderBean k = new OrderBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1. get a line
		String line = value.toString();

		// 2. cut the line
		String[] fields = line.split("\t");

		// 3. package object
		k.setOrder_id(Integer.parseInt(fields[0]));
		k.setPrice(Double.parseDouble(fields[2]));

		// 4. write out
		context.write(k, NullWritable.get());
	}

}
