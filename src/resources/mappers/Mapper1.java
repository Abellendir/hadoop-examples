package resources.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import driver.drivers.Driver.UpdateCount;
import resources.writables.CompositeKeyWritable;

public class Mapper1 extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!value.toString().isEmpty()) {
			context.getCounter(UpdateCount.CNT).increment(1);
			String id = value.toString().split("<====>")[1];
			if (!value.toString().split("<====>")[2].equals("")) {
				StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
				while (token.hasMoreTokens()) {
					String out = token.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
					if (!out.equals("")) {
						context.write(new CompositeKeyWritable(id, out), new IntWritable(1));
					}
				}
			}
		}
	}
}
