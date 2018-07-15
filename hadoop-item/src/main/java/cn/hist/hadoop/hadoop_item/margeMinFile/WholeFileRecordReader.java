package cn.hist.hadoop.hadoop_item.margeMinFile;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 实现RecordReader接口，最核心的就是处理好迭代多行文本的内容的逻辑，每次迭代通过调用nextKeyValue()方法来判断是否还有可读的文本行，直接设置当前的Key和Value，分别在方法getCurrentKey()和getCurrentValue()中返回对应的值。
 * 另外，我们设置了”map.input.file”的值是文件名称，以便在Map任务中取出并将文件名称作为键写入到输出。
 */
public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

	private FileSplit fileSplit;
	private JobContext jobContext;
	private NullWritable currentKey = NullWritable.get();
	private BytesWritable currentValue;
	private boolean finishConverting = false;

	@Override
	public void close() throws IOException {

	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	/**
	 * 直接设置当前的Key和Value，分别在方法getCurrentKey()和getCurrentValue()中返回对应的值。
	 */
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		float progress = 0;
		if (finishConverting) {
			progress = 1;
		}
		return progress;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.jobContext = context;
		context.getConfiguration().set("map.input.file", fileSplit.getPath().getName());
	}
	
	/**
	 * 每次迭代通过调用nextKeyValue()方法来判断是否还有可读的文本行
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!finishConverting) {
			currentValue = new BytesWritable();
			int len = (int) fileSplit.getLength();
			byte[] content = new byte[len];
			Path path = fileSplit.getPath();
			FileSystem fs = path.getFileSystem(jobContext.getConfiguration());
			FSDataInputStream in = null;
			try {
				in = fs.open(path);
				IOUtils.readFully(in, content, 0, len);
				currentValue.set(content, 0, len);
			} finally {
				if (in != null) {
					IOUtils.closeStream(in);
				}
			}
			finishConverting = true;
			return true;
		}
		return false;
	}
}