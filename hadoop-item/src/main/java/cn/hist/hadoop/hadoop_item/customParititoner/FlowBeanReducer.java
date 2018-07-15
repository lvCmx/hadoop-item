package cn.hist.hadoop.hadoop_item.customParititoner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	
	private long upFlowSum=0;
	private long downFlowSum=0;
	private long sum=0;
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> value,Context context)
			throws IOException, InterruptedException {
		Iterator<FlowBean> it = value.iterator();
		FlowBean bean =null;
		while(it.hasNext()) {
			bean = it.next();
			upFlowSum+=bean.getUpFlow();
			downFlowSum+=bean.getDownFlow();
		}
		sum=upFlowSum+downFlowSum;
		bean.setUpFlow(upFlowSum);
		bean.setDownFlow(downFlowSum);
		bean.setSumFlow(sum);
		
		context.write(key, bean);
	}
}
