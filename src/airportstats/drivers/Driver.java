package airportstats.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import airportstats.mappers.AirportCitySuptMapper;
import airportstats.mappers.CarrierSuptMapper;
import airportstats.mappers.FinalMapper;
import airportstats.mappers.PrimaryMapper;
import airportstats.mappers.TailNumSuptMapper;
import airportstats.partitioners.FinalPartitioner;
import airportstats.reducers.FinalReducer;
import airportstats.reducers.ParserReducer;
import airportstats.reducers.ReducerCombiner;



public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	
		
		job.setJarByClass(Driver.class);
		//job.setMapperClass(ParserMapperA.class);
		job.setCombinerClass(ReducerCombiner.class);
		job.setReducerClass(ParserReducer.class);
		job.setNumReduceTasks(25);
		//job.setPartitionerClass(Q1partitionerA.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PrimaryMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CarrierSuptMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AirportCitySuptMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, TailNumSuptMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		
		job.waitForCompletion(true);
		
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
		FileInputFormat.setInputPaths(jobNext, new Path(args[4]));
		FileOutputFormat.setOutputPath(jobNext, new Path(args[5]));
		
		
		System.exit(jobNext.waitForCompletion(true) ? 0 : 1);
		
	}
}
