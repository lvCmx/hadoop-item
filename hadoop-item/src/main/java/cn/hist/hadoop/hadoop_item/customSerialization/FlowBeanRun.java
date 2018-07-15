package cn.hist.hadoop.hadoop_item.customSerialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class FlowBeanRun {
	
	public static void main(String[] args) {
		Configuration config = new Configuration();
		try {
			Job job = Job.getInstance(config);
			job.setJarByClass(FlowBeanRun.class);
			job.setJobName("flowBeanRun");
			
			//设置map/reduce类
			job.setMapperClass(FlowBeanMapper.class);
			job.setReducerClass(FlowBeanReducer.class);
			
			//设置输入输出流
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//设置key、value的类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowBean.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
