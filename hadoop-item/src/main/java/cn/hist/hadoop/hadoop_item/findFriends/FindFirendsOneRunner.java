package cn.hist.hadoop.hadoop_item.findFriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 找出每个用户都是谁的好友，例：
 * Map端
 * 读一行A:B,C,D,E,F,O (A的好友有这些，反过来拆开，这些人中的每一个都是A的好友)
 * 输出：<B,A> <C,A> <D,A> <E,A> <O,A>
 * 读一行B:C,E,F,A
 * 输出：<C,B> <E,B> <F,B> <A,B>
 * 
 * Reduce端：
 * key相同的会分到一组:
 * <B,A> :B是key,A是 value
 * [C,<A,B>]说明：C是A与B的共同好友。
 */
public class FindFirendsOneRunner extends Configured implements Tool{

	static class FindFirendMapper extends Mapper<LongWritable,Text,Text, Text>{
		//A:B,C,D,E,F,O
		//<B,A> <C,A> <D,A> <E,A> <O,A>
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] keyAndVal = value.toString().split(":");
			String[] vals=keyAndVal[1].split(",");
			for (String v : vals) {
				context.write(new Text(v), new Text(keyAndVal[0]));
			}
		}
	}
	
	//生成
	//key:A
	//value:[B,C,D,E,F]
	//B-C,A
	//B-D,A
	//B-E,A
	//B-F,A
	//C-D,A
	//C-E,A
	//C-F,A
	//D-E,A
	//.....
	static class FindFirendReducer extends Reducer<Text, Text, Text, Text>{

		List<String> firends=new ArrayList<String>();
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			//将内容转成list
			for (Text text : value) {
				firends.add(text.toString());
			}
			for(int i=0;i<firends.size()-1;i++) {
				for(int j=i+1;j<firends.size();j++) {
					context.write(new Text(firends.get(i)+"-"+firends.get(j)), key);
				}
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		return 0;
	}
	public static void main(String[] args) {
		//ToolRunner.run
	}
}