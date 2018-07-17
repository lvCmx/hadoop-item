package cn.hist.hadoop.hadoop_item.itemMaxMoney;

import org.apache.hadoop.io.WritableComparator;

public class ItemGroupingComparator extends WritableComparator{

	public int compare(Object a, Object b) {
		Item item1=(Item)a;
		Item item2=(Item)b;
		return item1.getItemId().compareTo(item2.getItemId());
	}
}