package tfidf.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tfidf.drivers.Driver.UpdateCount;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
	
	private long count;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		this.count = context.getConfiguration().getLong(UpdateCount.CNT.name(), 0);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!value.toString().isEmpty()) {
			String[] values = value.toString().split("\t");
			String id = values[0];
			String unigram = values[1];
			String TFvalue = values[2];
			String n = values[3];
			double TFIDFvalue = Double.parseDouble(TFvalue) * Math.log10(count/Integer.parseInt(n));
			context.write(new Text(unigram),new Text(id + "\t" + TFvalue + "\t" + TFIDFvalue + "\t" + count));
		}
	}
}
