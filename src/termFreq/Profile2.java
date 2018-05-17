package termFreq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Profile2 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job0 = Job.getInstance(conf);

		job0.setJarByClass(Profile2.class);
		job0.setMapperClass(Profile2Mapper.class);
		job0.setReducerClass(Profile2Reducer.class);
		job0.setNumReduceTasks(9);
		job0.setPartitionerClass(Profile2Partitioner.class);
		job0.setGroupingComparatorClass(Profile2GroupComparator.class);
		job0.setOutputKeyClass(CompositeKeyWritable.class);
		job0.setOutputValueClass(IntWritable.class);
		job0.setInputFormatClass(TextInputFormat.class);
		job0.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job0, new Path(args[0]));
		FileOutputFormat.setOutputPath(job0, new Path(args[1]));
		System.exit(job0.waitForCompletion(true) ? 0 : 1);
	}

	public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

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

	public static class Profile2GroupComparator extends WritableComparator {
		public Profile2GroupComparator() {
			super(CompositeKeyWritable.class);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) o1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) o2;
			return key1.getNaturalKey().compareTo(key2.getNaturalKey());
		}
	}

	public static class Profile2Mapper extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (!value.toString().isEmpty()) {
				// System.out.println(value.toString());
				String id = value.toString().split("<====>")[1];
				if (!value.toString().split("<====>")[2].equals("")) {
					StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
					while (token.hasMoreTokens()) {
						String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
						if (!out.equals("")) {
							context.write(new CompositeKeyWritable(id, out), new IntWritable(1));
						}
					}
				}
			}
		}
	}

	public static class Profile2Partitioner extends Partitioner<CompositeKeyWritable, IntWritable> {
		@Override
		public int getPartition(CompositeKeyWritable key, IntWritable value, int numReducers) {
			System.out.println(key.toString());
			return Math.abs(key.getNaturalKey().hashCode() % numReducers);
		}
	}

	public static class Profile2Reducer
			extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {
		IntWritable result = new IntWritable();

		public void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			System.out.println(key.toString() + "\t" + sum);
			context.write(key, result);
		}
	}
}
