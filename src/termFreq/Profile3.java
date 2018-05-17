package termFreq;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Profile3 {
	private static final String OUTPUT_PATH = "temp";

	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job0 = Job.getInstance(conf);
		
		job0.setJarByClass(Profile3.class);
		job0.setMapperClass(UniqueValueMapper.class);
		job0.setReducerClass(IntSumReducer.class);
		job0.setNumReduceTasks(9);
		job0.setPartitionerClass(AlphabetRelativeFrequencyPartitioner.class);
		job0.setOutputKeyClass(Text.class);
		job0.setOutputValueClass(IntWritable.class);
		job0.setInputFormatClass(TextInputFormat.class);
		job0.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job0, new Path(args[0]));
		FileOutputFormat.setOutputPath(job0, new Path(OUTPUT_PATH));

		job0.waitForCompletion(true);
		

		//Configuration conf2 = new Configuration();
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Profile3.class);
		job1.setMapperClass(SortByValueMapper.class);
		job1.setReducerClass(DescendingOrderReducer.class);
		//job1.setNumReduceTasks(9);
		//job1.setPartitionerClass(AlphabetRelativeFrequencyPartitioner2.class);
		job1.setSortComparatorClass(SortComparator.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.setInputPaths(job1, new Path(OUTPUT_PATH));
		
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

	public static class UniqueValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (!value.toString().isEmpty()) {
				StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
				while (token.hasMoreTokens()) {
					String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
					context.write(new Text(out), new IntWritable(1));
				}
			}
		}

	}

	/**
	 * I have attempted to partition this on a relative Frequency distribution of
	 * words in the English language. Each partition represents roughly 9 to 12
	 * percent of common words in the English language
	 */
	public static class AlphabetRelativeFrequencyPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			if(key.toString().equals("")) {
				return 8;
			}
			if (numReduceTasks == 9) {
				Character partitionKey = key.toString().charAt(0);
				if (partitionKey == 'a')
					return 0;
				else if (partitionKey >= 'b' && partitionKey <= 'c')
					return 1;
				else if (partitionKey >= 'd' && partitionKey <= 'f')
					return 2;
				else if (partitionKey >= 'g' && partitionKey <= 'n')
					return 3;
				else if (partitionKey >= 'o' && partitionKey <= 'p')
					return 4;
				else if (partitionKey >= 'q' && partitionKey <= 's')
					return 5;
				else if (partitionKey == 't')
					return 6;
				else if (partitionKey >= 'u' && partitionKey <= 'z')
					return 7;
				else if (partitionKey >= '0' && partitionKey <= '9')
					return 8;
				else
					return 8;
			}
			return 0;
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class SortByValueMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		IntWritable frequency = new IntWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String ID = value.toString().split("\t")[0];
			String count = value.toString().split("\t")[1];
			context.write(new IntWritable(Integer.parseInt(count)),new Text(ID));
		}
	}
	
	public static class AlphabetRelativeFrequencyPartitioner2 extends Partitioner<IntWritable, Text> {
		@Override
		public int getPartition(IntWritable key, Text value, int numReduceTasks) {
			if(value.toString().equals("")) {
				return 8;
			}
			System.out.println(value.toString() + "\t" + key.toString());
			if (numReduceTasks == 9) {
				Character partitionKey = value.toString().charAt(0);
				if (partitionKey == 'a')
					return 0;
				else if (partitionKey >= 'b' && partitionKey <= 'c')
					return 1;
				else if (partitionKey >= 'd' && partitionKey <= 'f')
					return 2;
				else if (partitionKey >= 'g' && partitionKey <= 'n')
					return 3;
				else if (partitionKey >= 'o' && partitionKey <= 'p')
					return 4;
				else if (partitionKey >= 'q' && partitionKey <= 's')
					return 5;
				else if (partitionKey == 't')
					return 6;
				else if (partitionKey >= 'u' && partitionKey <= 'z')
					return 7;
				else if (partitionKey >= '0' && partitionKey <= '9')
					return 8;
				else
					return 8;
			}
			return 0;
		}
	}
	/*
	public static class Profile3Partitioner extends Partitioner<IntWritable, Text> {

		@Override
		public int getPartition(IntWritable key, Text value, int numReducers) {
			return key.get() % numReducers;
		}

	}*/

	public static class DescendingOrderReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		Text word = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				//System.out.println(key.toString() + "\t" + value.toString());
				for(Text value: values) {
					word.set(value);
					context.write(word,key);
				}
		}
	}
	
	public static class SortComparator extends WritableComparator{
		public SortComparator() {
			super(IntWritable.class,true);
		}
		
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			IntWritable k1 = (IntWritable) o1;
			IntWritable k2 = (IntWritable) o2;
			
			int cmp = k1.compareTo(k2);
			return -1*cmp;
		}
		
	}	
}
