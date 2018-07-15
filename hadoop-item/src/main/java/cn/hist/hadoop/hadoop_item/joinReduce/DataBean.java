package cn.hist.hadoop.hadoop_item.joinReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 要进行join操作的数据保存到这个地方。 用户信息，订单信息 用户信息 userid,name 订单信息 orderid,orderName,userId
 */
public class DataBean implements Writable{
	// 共同使用的用户id
	private String userId;
	// 用户其它信息
	private String userName;

	// 订单信息
	private String orderId;
	private String orderName;
 	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	private String status; //标志是用户信息还是订单信息 0:用户，1：订单
	
	public DataBean() {
	}
	
	public DataBean(String userId, String userName) {
		super();
		this.userId = userId;
		this.userName = userName;
	}

	public DataBean(String userId, String orderId, String orderName) {
		super();
		this.userId = userId;
		this.orderId = orderId;
		this.orderName = orderName;
	}
	
	public DataBean(String userId, String userName, String orderId, String orderName) {
		super();
		this.userId = userId;
		this.userName = userName;
		this.orderId = orderId;
		this.orderName = orderName;
	}
	
	public String getUserId() {
		return userId;
	}
	
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public String getUserName() {
		return userName;
	}
	
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public String getOrderId() {
		return orderId;
	}
	
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	
	public String getOrderName() {
		return orderName;
	}
	
	public void setOrderName(String orderName) {
		this.orderName = orderName;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.userId);
		out.writeUTF(this.userName);
		out.writeUTF(this.orderId);
		out.writeUTF(this.orderName);
	}

	public void readFields(DataInput in) throws IOException {
		this.userId = in.readUTF();
		this.userName = in.readUTF();
		this.orderId = in.readUTF();
		this.orderName = in.readUTF();
	}
	
	public String toString() {
		return userId + "\t" + userName + "\t" + orderId + "\t"+ orderName;
	}
}