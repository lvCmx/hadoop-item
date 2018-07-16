package cn.hist.hadoop.hadoop_item.hbasetohdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 从hdfs中读取数据，然后将其导入到hbase中。
 * hdfs中保存的数据的格式：
 * rowkey	faimly,qualifity,value:faimly1,qualifity1,value1
 */
public class HDFSToHBase extends Configured implements Tool{

	static class HDFSToHBaseMapper extends Mapper<LongWritable,Text, Text,Text>{
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] valArr = value.toString().split("\t");
			context.write(new Text(valArr[0]), new Text(valArr[1]));
		}
	}

	static class HDFSToHBaseReduce extends TableReducer<Text, Text, NullWritable>{
		protected void reduce(Text key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			Put put=new Put(Bytes.toBytes(key.toString()));
			for (Text text : value) {
				String[] columns = text.toString().split(":");
				for (String column : columns) {
					String[] cells = column.split(",");
					put.addColumn(Bytes.toBytes(cells[0]), Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]));
				}
			}
			context.write(NullWritable.get(), put);
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf());
		job.setJarByClass(HDFSToHBase.class);
		job.setJobName("HDFSToHBase");
		
		job.setMapperClass(HDFSToHBaseMapper.class);
		job.setReducerClass(HDFSToHBaseReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		TableMapReduceUtil.initTableReducerJob("student",HDFSToHBaseReduce.class, job);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			ToolRunner.run(conf, new HDFSToHBase(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}