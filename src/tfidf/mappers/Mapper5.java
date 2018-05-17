package tfidf.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper5 extends Mapper<LongWritable, Text, Text, NullWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
	}
}
