package cn.hist.hadoop.hadoop_item.itemMaxMoney;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemPartitioner extends Partitioner<Text, Item>{

	public int getPartition(Text key, Item val, int arg2) {
		return val.getItemId().hashCode()% 5;
	}
}
