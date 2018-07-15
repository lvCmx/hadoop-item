package cn.hist.hadoop.hadoop_item.customParititoner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class FlowBeanRun {
	
	public static void main(String[] args) throws Exception{
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(FlowBeanRun.class);
		job.setJobName("flowBeanPartitioner");
		
		//设置mr
		job.setMapperClass(FlowBeanMapper.class);
		job.setReducerClass(FlowBeanReducer.class);

		//指定自定义分区
		job.setPartitionerClass(FlowBeanPartitioner.class);
		
		//设置输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//设置输入输出的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		job.waitForCompletion(true);
	}
}
