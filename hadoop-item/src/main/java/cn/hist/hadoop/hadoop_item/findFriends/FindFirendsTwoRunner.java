package cn.hist.hadoop.hadoop_item.findFriends;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class FindFirendsTwoRunner extends Configured implements Tool{

	//读取，上个mr作业的输出
	static class FirendsMapper extends Mapper<LongWritable,Text, Text, Text>{
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			context.write(new Text(split[0]), new Text(split[1]));
		}
	}
	
	static class FirendsReduce extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			StringBuffer valBuffer = new StringBuffer("");
			while(iterator.hasNext()) {
				valBuffer.append(iterator.next().toString()+",");
			}
			String val = valBuffer.toString().substring(0,valBuffer.toString().length());
			context.write(key, new Text(val));
		}
	}
	
	public int run(String[] args) throws Exception {
		//省略
		return 0;
	}
	
	public static void main(String[] args) {
		//ToolRunner.run;   
	}
}
