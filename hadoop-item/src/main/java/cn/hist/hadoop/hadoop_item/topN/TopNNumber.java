package cn.hist.hadoop.hadoop_item.topN;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 求订单中的消费金额最大的前5个： 实现：定义一个长度为6的数组，将map读入的数据放入数组中，然后执行sort排序
 * 排序默认是按从小到大的顺序进行的，所以数组的第一个元素是最小的一个元素。
 */
public class TopNNumber extends Configured implements Tool {

	static class TopNMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		int n;
		OrderInfo topN[]=null;
		
		protected void setup(Context context)
				throws IOException, InterruptedException {
			n=Integer.parseInt(context.getConfiguration().get("map-top-N"));
			topN=new OrderInfo[n+1];
		}
		
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
			topN[0]=new OrderInfo(arr[0],Double.parseDouble(arr[1]));
			Arrays.sort(topN);
		}
		
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(int i=1;i<topN.length;i++) {
				OrderInfo orderInfo = topN[i];
				context.write(new Text(orderInfo.getName()),new DoubleWritable(orderInfo.getMoney()));
			}
		}
	}
	
	
	static class TopNReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		
		int n;
		OrderInfo topN[]=null;
		
		protected void setup(Context context)
				throws IOException, InterruptedException {
			n=Integer.parseInt(context.getConfiguration().get("map-top-N"));
			topN=new OrderInfo[n+1];
		}
		protected void reduce(Text key, Iterable<DoubleWritable> value,Context context)
				throws IOException, InterruptedException {
			for (DoubleWritable val : value) {
				topN[0]=new OrderInfo(key.toString(), val.get());
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(int i=topN.length;i>0;i--) {
				OrderInfo orderInfo = topN[i];
				context.write(new Text(orderInfo.getName()),new DoubleWritable(orderInfo.getMoney()));
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("map-top-N", "5");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopNNumber.class);
		job.setJobName("TopNNumber");

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			ToolRunner.run(conf, new TopNNumber(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}