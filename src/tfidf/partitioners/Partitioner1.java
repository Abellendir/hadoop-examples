package tfidf.partitioners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import tfidf.writables.CompositeKeyWritable;

public class Partitioner1 extends Partitioner<CompositeKeyWritable, IntWritable> {
	@Override
	public int getPartition(CompositeKeyWritable key, IntWritable value, int numReducers) {
		return Math.abs(key.getNaturalKey().hashCode() % numReducers);
	}
}
