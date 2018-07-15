package cn.hist.hadoop.hadoop_item.itemMaxMoney;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Item implements WritableComparable<Item>{

	private String itemId;
	private String prdId;
	private double money;

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getPrdId() {
		return prdId;
	}

	public void setPrdId(String prdId) {
		this.prdId = prdId;
	}

	public double getMoney() {
		return money;
	}

	public void setMoney(double money) {
		this.money = money;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.itemId);
		out.writeUTF(this.prdId);
		out.writeDouble(this.money);
	}

	public void readFields(DataInput in) throws IOException {
		this.itemId=in.readUTF();
		this.prdId=in.readUTF();
		this.money=in.readDouble();
	}

	//按订单id排序，如果id相同，则按照money排序。
	public int compareTo(Item o) {
		if(this.itemId.equals(o.getItemId())) {
			if(this.money>o.getMoney()) {
				return 1;
			}else if(this.money<o.getMoney()) {
				return -1;
			}else {
				return 0;
			}
		}
		return this.itemId.compareTo(o.getItemId());
	}
}