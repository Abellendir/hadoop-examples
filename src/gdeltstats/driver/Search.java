package gdeltstats.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import gdeltstats.combiner.SearchCombiner;
import gdeltstats.mapper.SearchMapper;
import gdeltstats.reducer.SearchReducer;

public class Search {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("search", args[2]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setCombinerClass(SearchCombiner.class);
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);
        //job.setNumReduceTasks(9);
        //job.setPartitionerClass(MyPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
