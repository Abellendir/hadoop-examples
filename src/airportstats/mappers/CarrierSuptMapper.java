package airportstats.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarrierSuptMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * Maps carrier names to the code for later use
	 * {CarrierName,carrier name}
	 * CarrierName is an identifier used to parse the info
	 */
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		if (!values.toString().isEmpty() && !values.toString().split(",")[0].equals("Code")) {
			String[] data = values.toString().split("\",\"");
			if (!data[0].replaceAll("\"","").equalsIgnoreCase("code")) {
				context.write(new Text("Carrier," + data[0].replaceAll("\"", "")), new Text("CarrierName," + data[1].replaceAll("\"", "")));
			}
		}
	}

}
