package cn.hist.hadoop.hadoop_item.customSerialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 如果只是需要使用hadoop的序列化功能，那就implements Writable就可以了
 * 如果我们需要对自定义的序列化类进行排序，重写其compleTo方法，则需要 extends WritableComparator
 */
public class FlowBean implements Writable{
	private String phoneNumber; //手机号
	private long upFlow; //上行流量
	private long downFlow; //下行流量
	private long sumFlow;	//上下行总和
	
	public FlowBean() {
	
	}
	public FlowBean(String phoneNumber,long upFlow, long downFlow, long sumFlow) {
		super();
		this.phoneNumber=phoneNumber;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = sumFlow;
	}
	
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
	public String getPhoneNumber() {
		return phoneNumber;
	}
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}
	/**
	 * 序列化方法
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.phoneNumber);
		out.writeLong(this.upFlow);
		out.writeLong(this.downFlow);
		out.writeLong(this.sumFlow);
	}

	/**
	 * 反序列化方法
	 */
	public void readFields(DataInput in) throws IOException {
		this.phoneNumber=in.readUTF();
		this.upFlow=in.readLong();
		this.downFlow=in.readLong();
		this.sumFlow=in.readLong();
	}
}
