package resources.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

	private String naturalKey;
	private String naturalValue;

	public String getNaturalKey() {
		return naturalKey;
	}

	public void setNaturalKey(String naturalKey) {
		this.naturalKey = naturalKey;
	}

	public String getNaturalValue() {
		return naturalValue;
	}

	public void setNaturalValue(String naturalValue) {
		this.naturalValue = naturalValue;
	}

	public CompositeKeyWritable() {

	}

	public CompositeKeyWritable(String naturalKey, String naturalValue) {
		this.naturalKey = naturalKey;
		this.naturalValue = naturalValue;
	}

	@Override
	public int compareTo(CompositeKeyWritable o) {
		int result = this.naturalKey.compareTo(o.getNaturalKey());
		if (result == 0) {
			result = this.naturalValue.compareTo(o.getNaturalValue());
		}
		return -1 * result;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.naturalKey = WritableUtils.readString(arg0);
		this.naturalValue = WritableUtils.readString(arg0);

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, naturalKey);
		WritableUtils.writeString(arg0, naturalValue);
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(naturalKey).append("\t").append(naturalValue).toString());
	}

}
