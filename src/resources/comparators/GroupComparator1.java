package resources.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import resources.writables.CompositeKeyWritable;

public class GroupComparator1 extends WritableComparator {

	public GroupComparator1() {
		super(CompositeKeyWritable.class);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) o1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) o2;
		return key1.getNaturalKey().compareTo(key2.getNaturalKey());
	}
}