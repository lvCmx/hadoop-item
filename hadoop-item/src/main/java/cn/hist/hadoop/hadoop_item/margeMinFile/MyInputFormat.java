package cn.hist.hadoop.hadoop_item.margeMinFile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable>{
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		//设置文件不可分片，保证每一个小文件生成一个key-value键值对
		return false;
	}
	
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		WholeFileRecordReader wholeFileRecordReader = new WholeFileRecordReader();
		wholeFileRecordReader.initialize(split, context);
		return wholeFileRecordReader;
	}
}