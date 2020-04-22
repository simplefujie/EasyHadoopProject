package com.fujie.mapreduce.partitionSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

// 1. Implement the writable interface
public class FlowBean implements WritableComparable<FlowBean> {

	private long upFlow;
	private long downFlow;
	private long sumFlow;

	// When deserializing, you need to call the empty parameter constructor, so you
	// must have
	public FlowBean() {
		super();
	}

	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	// Write serialization method
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	// Deserialization method
	// The read sequence of the deserialization method must be the same as the write
	// sequence of the write serialization method
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	// Write toString method to facilitate subsequent printing to text
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	// set and get methods
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	public void set(long upFlow2, long downFlow2) {
		this.upFlow = upFlow2;
		this.downFlow = downFlow2;
		this.sumFlow = upFlow2 + downFlow2;
	}

	// Add compare function
	public int compareTo(FlowBean bean) {

		int result;
		// According to the total flow size, in reverse order
		if (sumFlow > bean.getSumFlow()) {
			result = -1;
		} else if (sumFlow < bean.getSumFlow()) {
			result = 1;
		} else {
			result = 0;
		}

		return result;

	}
}
