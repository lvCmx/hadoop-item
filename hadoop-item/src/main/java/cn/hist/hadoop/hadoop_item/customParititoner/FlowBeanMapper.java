package cn.hist.hadoop.hadoop_item.customParititoner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 分别统计上行流量与下行流量
 */
public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		StringTokenizer token = new StringTokenizer(val);
		FlowBean bean = new FlowBean();
		String nextToken2 = token.nextToken();
		String nextToken = nextToken2;
		String phoneNumber = nextToken;
		bean.setPhoneNumber(phoneNumber);
		String upFlow= token.nextToken();
		bean.setUpFlow(Long.parseLong(upFlow));
		String downFlow= token.nextToken();
		bean.setDownFlow(Long.parseLong(downFlow));
		context.write(new Text(nextToken2), bean);
	}
}