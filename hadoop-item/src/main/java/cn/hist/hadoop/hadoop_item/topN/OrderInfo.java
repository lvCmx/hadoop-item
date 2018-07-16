package cn.hist.hadoop.hadoop_item.topN;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 订单信息
 */
public class OrderInfo implements WritableComparable<OrderInfo>{

	private String name;
	private double money;
	
	public OrderInfo() {
		super();
	}

	public OrderInfo(String name, double money) {
		super();
		this.name = name;
		this.money = money;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getMoney() {
		return money;
	}

	public void setMoney(double money) {
		this.money = money;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeDouble(this.money);
	}

	public void readFields(DataInput in) throws IOException {
		this.name=in.readUTF();
		this.money=in.readDouble();
	}

	//按照金额的升序排序
	public int compareTo(OrderInfo o) {
		if(this.money<o.getMoney()) {
			return -1;
		}else {
			return 1;
		}
	}
}
