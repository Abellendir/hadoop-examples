package tfidf.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tfidf.mappers.Mapper1;
import tfidf.mappers.Mapper2;
import tfidf.mappers.Mapper3;
import tfidf.mappers.Mapper4;
import tfidf.mappers.MapperFinal1;
import tfidf.mappers.MapperFinal2;
import tfidf.partitioners.GeneralPartitioner;
import tfidf.partitioners.Partitioner1;
import tfidf.reducers.Reducer1;
import tfidf.reducers.Reducer2;
import tfidf.reducers.Reducer3;
import tfidf.reducers.Reducer4;
import tfidf.reducers.Reducer5;
import tfidf.writables.CompositeKeyWritable;

public class Driver {

	private static final String JOB_PATH1 = "Mapper1";
	private static final String JOB_PATH2 = "Mapper2";
	private static final String JOB_PATH3 = "Mapper3";
	private static final String JOB_PATH4 = "Mapper4";

	public static enum UpdateCount {
		CNT
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf);

		job1.setJarByClass(Driver.class);
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setNumReduceTasks(16);
		job1.setPartitionerClass(Partitioner1.class);
		//job1.setGroupingComparatorClass(GroupComparator1.class);
		job1.setOutputKeyClass(CompositeKeyWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(JOB_PATH1));

		job1.waitForCompletion(true);
		

		long count = job1.getCounters().findCounter(UpdateCount.CNT).getValue();
		
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setNumReduceTasks(16);
		job2.setPartitionerClass(GeneralPartitioner.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(JOB_PATH1));
		FileOutputFormat.setOutputPath(job2, new Path(JOB_PATH2));

		job2.waitForCompletion(true);
		
		//long cn = job2.getCounters().findCounter(UpdateCount.CNT).getValue();
		
		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(Driver.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setNumReduceTasks(16);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(JOB_PATH2));
		FileOutputFormat.setOutputPath(job3, new Path(JOB_PATH3));

		job3.waitForCompletion(true);
		
		Job job4 = Job.getInstance(conf);
		job4.getConfiguration().setLong(UpdateCount.CNT.name(), count);
		job4.setJarByClass(Driver.class);
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		job4.setNumReduceTasks(16);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(JOB_PATH3));
		FileOutputFormat.setOutputPath(job4, new Path(JOB_PATH4));

		job4.waitForCompletion(true);
		
		Job job5 = Job.getInstance(conf);
		job5.setJarByClass(Driver.class);
		job5.setReducerClass(Reducer5.class);
		job5.setNumReduceTasks(16);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job5, new Path(JOB_PATH4), TextInputFormat.class, MapperFinal1.class);
		MultipleInputs.addInputPath(job5, new Path(args[0]), TextInputFormat.class, MapperFinal2.class);
		//job5.setInputFormatClass(TextInputFormat.class);
		//job5.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.setInputPaths(job5, new Path(args[0]));
		FileOutputFormat.setOutputPath(job5, new Path(args[1]));/*
		*/
		
		job5.waitForCompletion(true);
		long cn = job4.getCounters().findCounter(UpdateCount.CNT).getValue();
		System.out.println("JOB 1 Count " + job1.getCounters().findCounter(UpdateCount.CNT).getValue());
		System.out.println("JOB 2 count " + count);
		System.out.println("END COUNT " + cn);
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
