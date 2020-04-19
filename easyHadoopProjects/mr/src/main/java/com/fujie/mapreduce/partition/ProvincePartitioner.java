package com.fujie.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {

		// 1. Get first 3 numbers of phone number
		String preNum = key.toString().substring(0, 3);

		int partition = 4;
		// 2. Verify province
		if ("136".equals(preNum)) {
			partition = 0;
		} else if ("137".equals(preNum)) {
			partition = 1;
		} else if ("138".equals(preNum)) {
			partition = 2;
		} else if ("139".equals(preNum)) {
			partition = 3;
		}
		return partition;
	}
}
