package cn.hist.hadoop.hadoop_item.hbasetohdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 通过mapreduce实现hbase表中的数据导入到另一张表中。
 * 
 * 
 * HBase现数据格式如下：
 * rk001	baseinfo:name=sxl,baseinfo:age=20,extinfo.address=zz
 */
public class HBaseToHBase extends Configured implements Tool{

	//TableMapper只能自定义output key与value
	static class HBaseToHBaseMapper extends TableMapper<Text, Text>{
		/**
		 * 读取到的数据封装在value里面，即Result里面。
		 */
		private Text rowKey=new Text();
		@Override
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

	//将map的结果封装成put对象
	static class HBaseToHBaseReduce extends TableReducer<Text,Text, NullWritable>{
		private Put put=null;
		protected void reduce(Text key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			put=new Put(Bytes.toBytes(key.toString()));
			//读取map的结果数据
			String valStr=value.iterator().next().toString();
			String[] valArr = valStr.split(" ");
			for (String v : valArr) {
				String[] familyAndQualifier = v.split(":");
				String[] QualifierAndValue = familyAndQualifier[1].split("=");
				put.addColumn(Bytes.toBytes(familyAndQualifier[0]),
						Bytes.toBytes(QualifierAndValue[0]),
						Bytes.toBytes(QualifierAndValue[1]));
			}
			context.write(NullWritable.get(),put);
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(HBaseToHBase.class);
		job.setJobName("HBaseToHBase");
		
		String hbaseTable="student";
		String toHBaseTable="tostudent";
		
		Scan scan = new Scan();
		//TableMapper端的配置
		TableMapReduceUtil.initTableMapperJob(hbaseTable, scan, HBaseToHBaseMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(toHBaseTable, HBaseToHBaseReduce.class, job);
		
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			ToolRunner.run(conf, new HBaseToHBase(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}