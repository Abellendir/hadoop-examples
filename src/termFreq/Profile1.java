package termFreq;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main Class
 * 
 * @author adam Bellendir
 *
 * And this one works 
 */
public class Profile1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Profile1.class);
		job.setMapperClass(UniqueValueMapper.class);
		job.setReducerClass(UniqueValueReducer.class);
		job.setNumReduceTasks(9);
		job.setPartitionerClass(AlphabetRelativeFrequencyPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class UniqueValueMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (!value.toString().isEmpty()) {
				StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
				while (token.hasMoreTokens()) {
					String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
					context.write(new Text(out), NullWritable.get());
				}
			}
		}

	}

	/**
	 * I have attempted to partition this on a relative Frequency distribution of
	 * words in the English language. Each partition represents roughly 9 to 12
	 * percent of common words in the English language
	 */
	public static class AlphabetRelativeFrequencyPartitioner extends Partitioner<Text, NullWritable> {
		@Override
		public int getPartition(Text key, NullWritable value, int numReduceTasks) {
			if(key.toString().equals("")) {
				return 8;
			}
			if (numReduceTasks == 9) {
				System.out.println(key.toString());
				Character partitionKey = key.toString().charAt(0);
				System.out.println(partitionKey);
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

	public static class UniqueValueReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

}
