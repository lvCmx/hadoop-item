package cn.hist.hadoop.hadoop_item.outputMultipFile;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 需求：现有数据如下：
 * sirenxing424@126.com
   lixinyu23@qq.com
   chenlei1201@gmail.com
   370433835@qq.com
   统计各个邮箱出现的次数，并且将它们分别输出到不同类别的文件中
 */
public class OutputMuitipFile extends Configured implements Tool{
	
	static class MultipFileMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String string = value.toString();
			context.write(new Text(string),new IntWritable(1));
		}
	}
	
	static class MultipFileReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private MultipleOutputs<Text, IntWritable> multiple=null;

		protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			multiple=new MultipleOutputs<Text, IntWritable>(context);
		}
		
		protected void reduce(Text key, Iterable<IntWritable> it,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			//解析出邮箱的类型
			String keyStr = key.toString();
			int at = keyStr.indexOf("@");
			int point = keyStr.indexOf(".");
			String prefix = keyStr.substring(at,point+1);
			
			//将邮箱个数相加
			int sum=0;
			Iterator<IntWritable> iterator = it.iterator();
			while(iterator.hasNext()) {
				IntWritable next = iterator.next();
				sum+=next.get();
			}
			multiple.write(key,new IntWritable(sum), prefix);
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			multiple.close();
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		
		//job相关的配置....
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new Configuration(), new OutputMuitipFile(), args);
		System.exit(run);
	}
}