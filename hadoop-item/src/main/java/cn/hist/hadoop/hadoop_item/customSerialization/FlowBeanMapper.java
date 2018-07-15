package cn.hist.hadoop.hadoop_item.customSerialization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 为了实验我们自定义的数据类型，我们只进行一个简单的流量求和运行就行了。 数据示例： 手机号码 上行流量 下行流量
 */
public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//读取的每一行记录
		String[] values = value.toString().split("\t");
		//将上行流量与下行流量相加，即总流量
		context.write(new Text(values[0]),
				new FlowBean(
						values[0],
						Long.parseLong(values[1]),
						Long.parseLong(values[2]),
						Long.parseLong(values[1])+Long.parseLong(values[2])
					)
				);
	}
}
