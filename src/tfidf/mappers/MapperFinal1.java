package tfidf.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFinal1 extends Mapper<LongWritable,Text,Text,Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		if(!value.toString().isEmpty()) {
			String[] s = value.toString().split("\t");
			String id = s[0];
			String unigram = s[1];
			String TFIDFvalue = s[3];
			context.write(new Text(id),new Text("word<====>" + unigram + "<====>" + TFIDFvalue));
		}
	}
}
