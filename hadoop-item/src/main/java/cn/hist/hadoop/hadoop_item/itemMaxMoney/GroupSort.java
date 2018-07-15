package cn.hist.hadoop.hadoop_item.itemMaxMoney;

import java.io.IOException;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 由于我们前面相同订单id的bean是按照成交额由高到低排的，那么这个时候传进来的key就肯定是成交额最大的bean。
 */
public class GroupSort {
	
	static class SortMapper extends Mapper<LongWritable, Text, Item, NullWritable>{
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Item item = new Item();
			String[] values = value.toString().split(" ");
			item.setItemId(values[0]);
			item.setPrdId(values[1]);
			item.setMoney(Double.parseDouble(values[2]));
			
			context.write(item, NullWritable.get());
		}
	}
	
	static class SortReducer extends Reducer<Item, NullWriter, Item,NullWritable>{
		@Override
		protected void reduce(Item key, Iterable<NullWriter> value,Context context)
				throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(GroupSort.class);
		job.setJobName("GroupSort");
		
		job.setGroupingComparatorClass(ItemGroupingComparator.class);
		job.setPartitionerClass(ItemPartitioner.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(Item.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Item.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}