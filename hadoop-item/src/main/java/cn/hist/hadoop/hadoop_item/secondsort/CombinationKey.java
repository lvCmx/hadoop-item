package cn.hist.hadoop.hadoop_item.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Administrator
 *
 */
public class CombinationKey implements WritableComparable<CombinationKey> {

	private static final Logger logger = LoggerFactory.getLogger(CombinationKey.class);

	private Text firstKey;
	private IntWritable secondKey;

	public CombinationKey() {
		firstKey = new Text();
		secondKey = new IntWritable();
	}

	public Text getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(Text firstKey) {
		this.firstKey = firstKey;
	}

	public IntWritable getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(IntWritable secondKey) {
		this.secondKey = secondKey;
	}

	public void write(DataOutput out) throws IOException {
		this.firstKey.write(out);
		this.secondKey.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.firstKey.readFields(in);
		this.secondKey.readFields(in);
	}

	public int compareTo(CombinationKey o) {
		return this.firstKey.compareTo(o.getFirstKey());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstKey == null) ? 0 : firstKey.hashCode());
		result = prime * result + ((secondKey == null) ? 0 : secondKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CombinationKey other = (CombinationKey) obj;
		if (firstKey == null) {
			if (other.firstKey != null)
				return false;
		} else if (!firstKey.equals(other.firstKey))
			return false;
		if (secondKey == null) {
			if (other.secondKey != null)
				return false;
		} else if (!secondKey.equals(other.secondKey))
			return false;
		return true;
	}
}