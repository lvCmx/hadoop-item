package cn.hist.hadoop.hadoop_item.joinReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinReduceRun extends Configured implements Tool{

	//map / reduce 读取目录下的数据，目录下包括用户数据与订单数据。
	static class JoinMapper extends Mapper<LongWritable, Text, Text, DataBean>{
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split(",");
			DataBean dataBean = null;
			if(values.length==2) {
				//用户信息
				//用户id，用户名称
				dataBean = new DataBean(values[0],values[1]);
				dataBean.setStatus("0");
			}else if(values.length==3) {
				//订单信息
				//用户id，订单id,订单名称
				dataBean = new DataBean(values[0],values[1],values[2]);
				dataBean.setStatus("1");
			}
			context.write(new Text(dataBean.getUserId()),dataBean);
		}
	}
	
	static class JoinReduce extends Reducer<Text, DataBean, Text, DataBean>{
		//记录所有订单信息
		List<DataBean> list=new ArrayList<DataBean>();
		protected void reduce(Text key, Iterable<DataBean> it,Context context)
				throws IOException, InterruptedException {
			Iterator<DataBean> iterator = it.iterator();
			while(iterator.hasNext()) {
				DataBean data = iterator.next();
				//订单信息
				if("1".equals(data.getStatus())){
					list.add(data);
				}
			}
			//获取所有的用户信息，为其添加订单信息
			for (DataBean dataBean : it) {
				if("0".equals(dataBean.getStatus())){
					for (DataBean order : list) {
						dataBean.setOrderId(order.getOrderId());
						dataBean.setOrderName(order.getOrderName());
						context.write(key,dataBean);
					}
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		
		job.setJobName("JoinReduceRun");
		job.setJarByClass(JoinReduceRun.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ToolRunner.run(conf, new JoinReduceRun(), args);
	}
}
