package resources.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!value.toString().isEmpty()) {
			String[] values = value.toString().split("\t");
			String id = values[0];
			String unigram = values[1];
			String frequency = values[2];
			String TFvalue = values[3];
			context.write(new Text(unigram),new Text(id + "\t" + TFvalue));
		}
	}
}
