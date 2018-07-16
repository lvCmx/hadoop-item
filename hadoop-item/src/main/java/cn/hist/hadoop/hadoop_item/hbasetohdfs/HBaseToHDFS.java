package cn.hist.hadoop.hadoop_item.hbasetohdfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 通过mapreduce实现hbase表中的数据导入到HDFS。
 * 
 * 
 * HBase现数据格式如下：
 * rk001	baseinfo:name=sxl,baseinfo:age=20,extinfo.address=zz
 */
public class HBaseToHDFS extends Configured implements Tool{
	
	static class HBaseToHDFSMapper extends TableMapper<Text, Text>{
		private Text rowKey=new Text();
		protected void map(ImmutableBytesWritable key, Result value,Context context)
				throws IOException, InterruptedException {
			//读取到rowkey
			rowKey.set(Bytes.toString(value.getRow()));
			//读取所有的列单元
			Cell[] rawCells = value.rawCells();
			StringBuffer resultBuf = new StringBuffer();
			for (Cell cell : rawCells) {
				//读取列族
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				//读取列名
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//读取列值
				String val = Bytes.toString(CellUtil.cloneValue(cell));
				resultBuf.append(family+":"+qualifier+"="+val+" ");
			}
			String resultStr = resultBuf.toString().trim();
			context.write(rowKey,new Text(resultStr));
		}
	}
	
	static class HBaseToHDFSReduce extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it= value.iterator();
			while(it.hasNext()) {
				Text next = it.next();
				context.write(key,next);
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf());
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(HBaseToHDFSMapper.class);
		job.setReducerClass(HBaseToHDFSReduce.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		Scan scan = new Scan();
		//可以添加一些查询条件
	    //scan.addColumns(columns);  
		//scan.setFilter(new FirstKeyOnlyFilter());  
		
		//table, scan, mapper, outputKeyClass, outputValueClass, job
		TableMapReduceUtil.initTableMapperJob(
				"stu", scan, HBaseToHDFSMapper.class, Text.class, Text.class, job);
		//table, reducer, job
		//TableMapReduceUtil.initTableReducerJob("stu", ExportReducer.class, job);
		boolean b=job.waitForCompletion(true);
		if(b){
			return 0;
		}else{
			return -1;
		}
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			ToolRunner.run(conf, new HBaseToHDFS(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}