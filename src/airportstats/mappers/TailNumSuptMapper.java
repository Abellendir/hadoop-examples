package airportstats.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TailNumSuptMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		if (!values.toString().isEmpty()) {
			String[] data = values.toString().split(",");
			if (!data[0].equalsIgnoreCase("tailnum") && !data[0].equals("") && data.length != 1 && !data[data.length-1].equals("None")) {
				context.write(new Text("Plane," + data[0]), new Text("Year," + data[data.length - 1]));
			}
		}
	}

}
