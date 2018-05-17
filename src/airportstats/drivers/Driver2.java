package airportstats.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import airportstats.mappers.FinalMapper;
import airportstats.partitioners.FinalPartitioner;
import airportstats.reducers.FinalReducer;

public class Driver2 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf2 = new Configuration();
		Job jobNext = Job.getInstance(conf2);
		
		jobNext.setJarByClass(Driver.class);
		jobNext.setReducerClass(FinalReducer.class);
		jobNext.setNumReduceTasks(8);
		jobNext.setMapperClass(FinalMapper.class);
		jobNext.setPartitionerClass(FinalPartitioner.class);
		jobNext.setOutputKeyClass(Text.class);
		jobNext.setOutputValueClass(Text.class);
		jobNext.setMapOutputKeyClass(Text.class);
		jobNext.setMapOutputValueClass(Text.class);
		jobNext.setInputFormatClass(TextInputFormat.class);
		jobNext.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobNext, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobNext, new Path(args[1]));
		
		
		System.exit(jobNext.waitForCompletion(true) ? 0 : 1);
	}

}
