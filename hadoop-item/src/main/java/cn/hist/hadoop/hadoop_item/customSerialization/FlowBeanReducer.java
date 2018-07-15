package cn.hist.hadoop.hadoop_item.customSerialization;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowBeanReducer extends Reducer<Text,FlowBean, Text, LongWritable>{
	
	private long sum= 0L;
	@Override
	protected void reduce(Text text, Iterable<FlowBean> val,Context context)
			throws IOException, InterruptedException {
		Iterator<FlowBean> it = val.iterator();
		while(it.hasNext()) {
			FlowBean flowBean = it.next();
			sum+=flowBean.getSumFlow();
		}
		context.write(text, new LongWritable(sum));
	}
}