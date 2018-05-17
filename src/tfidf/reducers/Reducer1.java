package tfidf.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import tfidf.writables.CompositeKeyWritable;


public class Reducer1 extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {
	
	private IntWritable result = new IntWritable();
	
	public void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
