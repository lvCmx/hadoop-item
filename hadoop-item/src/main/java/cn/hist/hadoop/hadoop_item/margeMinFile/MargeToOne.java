package cn.hist.hadoop.hadoop_item.margeMinFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * 实现将小文件合并成一个大文件
 * 只需要一个map，而不需要reduce
 */
public class MargeToOne{
	//map
	static class MargeToOneMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{
		private Text keyText = null;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//获取文件的名称
			String fileName = context.getConfiguration().get("map.input.file");
			keyText=new Text(fileName);
		}
		
		protected void map(NullWritable key, BytesWritable value,Context context)
				throws IOException, InterruptedException {
			context.write(keyText,value);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MargeToOne.class);
		job.setMapperClass(MargeToOneMapper.class);
		
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		
		//设置reduceTask个为3
		//它同时表示结果将输出3个文件。
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}