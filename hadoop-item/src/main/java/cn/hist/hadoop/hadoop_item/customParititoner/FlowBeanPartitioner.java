package cn.hist.hadoop.hadoop_item.customParititoner;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowBeanPartitioner extends Partitioner<Text, FlowBean>{
	
	//模拟数据库，假如数据库中有如下手机区号
	private static Map<String,Integer> phonePrefx=new HashMap<String,Integer>();
	static {
		phonePrefx.put("150", 0);
		phonePrefx.put("135", 1);
		phonePrefx.put("182", 2);
		phonePrefx.put("171", 3);
	}
	
	public int getPartition(Text text, FlowBean bean, int arg2) {
		String prefix = text.toString().substring(0, 4);
		//4表示其它。
		Integer id =4;
		if(phonePrefx.containsKey(prefix)) {
			id  = phonePrefx.get(prefix);
		}
		return id;
	}
}
