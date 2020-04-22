package com.fujie.mapreduce.partitionSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

	FlowBean bean = new FlowBean();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 ��ȡһ��
		String line = value.toString();

		// 2 ��ȡ
		String[] fields = line.split("\t");

		// 3 ��װ����
		String phoneNbr = fields[0];
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);

		bean.set(upFlow, downFlow);
		v.set(phoneNbr);

		// 4 ���
		context.write(bean, v);
	}
}
