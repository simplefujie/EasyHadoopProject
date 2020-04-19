package com.fujie.mapreduce.partition;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context content)
			throws IOException, InterruptedException {
		long sum_upFlow = 0;
		long sum_downFlow = 0;

		// 1. Traverse the used beans and accumulate the upstream traffic and downstream
		// traffic separately
		for (FlowBean flowBean : values) {
			sum_upFlow += flowBean.getUpFlow();
			sum_downFlow += flowBean.getDownFlow();
		}

		// 2. Package object
		FlowBean resutlBean = new FlowBean(sum_upFlow, sum_downFlow);

		// 3. Write out
		content.write(key, resutlBean);
	}
}
