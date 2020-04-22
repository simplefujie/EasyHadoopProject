package com.fujie.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private int order_id; // ¶©µ¥idºÅ
	private double price; // ¼Û¸ñ

	public OrderBean(int order_id, double price) {
		super();
		this.order_id = order_id;
		this.price = price;
	}

	public OrderBean() {
		super();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(order_id);
		out.writeDouble(price);
	}

	public void readFields(DataInput in) throws IOException {
		order_id = in.readInt();
		price = in.readDouble();
	}

	// sort
	public int compareTo(OrderBean o) {
		int result;

		if (order_id > o.getOrder_id()) {
			result = 1;
		} else if (order_id < o.getOrder_id()) {
			result = -1;
		} else {
			// Secondary sort
			result = price > o.getPrice() ? -1 : 1;
		}

		return result;

	}

	@Override
	public String toString() {
		return order_id + "\t" + price;
	}

	public int getOrder_id() {
		return order_id;
	}

	public void setOrder_id(int order_id) {
		this.order_id = order_id;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

}
