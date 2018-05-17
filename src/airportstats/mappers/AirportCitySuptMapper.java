package airportstats.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportCitySuptMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * Parsers airport name and city name
	 * AirportName identifier for reducer
	 * City identifier for reducer
	 * 
	 */
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		if (!values.toString().isEmpty()) {
			String [] data = values.toString().split(",");
			if (!data[0].equalsIgnoreCase("\"iata\"")) {
				context.write(new Text("Q7," + data[0].replaceAll("\"", "")), new Text("AirportName," + data[1].replaceAll("\"", "")));
				context.write(new Text("Airport," + data[0].replaceAll("\"", "")), new Text("AirportName," + data[1].replaceAll("\"", "")));
				context.write(new Text("Weather Airport," + data[0].replaceAll("\"", "")), new Text("City," + data[2].replaceAll("\"", "")));
			}
		}
	}

}
