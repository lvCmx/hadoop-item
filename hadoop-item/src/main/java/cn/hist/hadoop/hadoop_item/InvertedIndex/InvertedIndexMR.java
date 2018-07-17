package cn.hist.hadoop.hadoop_item.InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 数据文件：
 * a.txt
 * 	mapredice is simple
 * b.txt
 * 	hello mapreduce is very good
 */
public class InvertedIndexMR extends Configured implements Tool{

	static class InvertedIndexMapper extends Mapper<LongWritable,Text, Text,Text>{
		
		private Text keyInfo=new Text(); //存储单词和URL组合
		private Text valueInfo=new Text();//存储词频
		private FileSplit split;//存储split对象
		
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			split=(FileSplit) context.getInputSplit();
			
			StringTokenizer str=new StringTokenizer(value.toString());
			while(str.hasMoreTokens()) {
				//key值由单词和URL组成，如：mapreduce:a.txt
				//获取文件的完整路径
				int fileIndex = split.getPath().toString().indexOf("file");
				keyInfo.set(str.nextToken()+":"+split.getPath().toString().substring(fileIndex));
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
			}
		}
	}

	/**
	 * 需要对map的结果进行合并，combine
	 * 在map中，key是单词和url的组合
	 * 在combine时，我们将key=单词，value为url和词频的组合
	 */
	static class InvertedIndexCombine extends Reducer<Text,Text,Text,Text>{
		private Text info=new Text();
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			
			int sum=0;
			String[] split = key.toString().split(":");
			for (Text v : value) {
				sum=sum+Integer.parseInt(v.toString());
			}
			key.set(split[0]);
			info.set(split[1]+":"+sum);
			context.write(key, info);
		}
	}
	
	static class InvertedIndexReduce extends Reducer<Text,Text,Text,Text>{
		
		private Text result=new Text();
		protected void reduce(Text key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			StringBuffer strBuf = new StringBuffer();
			for (Text v : value) {
				strBuf.append(v+";");
			}
			result.set(strBuf.toString());
			context.write(key, result);
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("mapred.jar", "invertedIndex.jar");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(InvertedIndexMR.class);
		job.setJobName("InvertedIndexMR");
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombine.class);
		job.setReducerClass(InvertedIndexReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			ToolRunner.run(conf, new InvertedIndexMR(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
