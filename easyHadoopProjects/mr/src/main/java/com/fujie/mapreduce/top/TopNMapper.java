package com.fujie.mapreduce.top;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

	// ����һ��TreeMap��Ϊ�洢���ݵ���������Ȼ��key����
	private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();
	private FlowBean kBean;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		kBean = new FlowBean();
		Text v = new Text();

		// 1 ��ȡһ��
		String line = value.toString();

		// 2 �и�
		String[] fields = line.split("\t");

		// 3 ��װ����
		String phoneNum = fields[0];
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		long sumFlow = Long.parseLong(fields[3]);

		kBean.setDownFlow(downFlow);
		kBean.setUpFlow(upFlow);
		kBean.setSumFlow(sumFlow);

		v.set(phoneNum);

		// 4 ��TreeMap���������
		flowMap.put(kBean, v);

		// 5 ����TreeMap��������������10����ɾ����������С��һ������
		if (flowMap.size() > 10) {
//		flowMap.remove(flowMap.firstKey());
			flowMap.remove(flowMap.lastKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		// 6 ����treeMap���ϣ��������
		Iterator<FlowBean> bean = flowMap.keySet().iterator();

		while (bean.hasNext()) {

			FlowBean k = bean.next();

			context.write(k, flowMap.get(k));
		}
	}
}
