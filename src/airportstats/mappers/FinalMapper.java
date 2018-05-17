package airportstats.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		if (!values.toString().isEmpty()) {
			String[] data = values.toString().split("\t");
			String[] keys = data[0].split(",");
			String[] vars = data[1].split(",");
			switch (keys[0]) {
			case "Day":
				context.write(
						new Text("Q1A2a,Day" ),
						new Text(keys[1] + "," + Float.toString(Float.parseFloat(vars[0]) / Float.parseFloat(vars[1]))));
				break;
			case "Month":
				context.write(
						new Text("Q1A2b,Month"),
						new Text(keys[1]+ "," + Float.toString(Float.parseFloat(vars[0]) / Float.parseFloat(vars[1]))));
				break;
			case "Time":
				context.write(
						new Text("Q1A2c,Time"),
						new Text(keys[1]+ "," + Float.toString(Float.parseFloat(vars[0]) / Float.parseFloat(vars[1]))));
				break;
			case "Airport":
				context.write(new Text("Q3," + vars[0]), new Text(keys[keys.length-1] + "," + vars[vars.length-1]));
				break;
			case "Carrier":
				if (Integer.parseInt(vars[1]) != 0) {
					context.write(
							new Text(
									"Q4," + Float.toString(Float.parseFloat(vars[0]) / Float.parseFloat(vars[1]))),
							new Text(keys[1] + "," + vars[1] + "," + vars[0]));
				}
				break;
			case "Plane":
				if (Integer.parseInt(vars[2]) != 0) {
					context.write(new Text("Q5," + vars[0]),
							new Text(keys[1] + ","
									+ Float.toString(Integer.parseInt(vars[1]) / Integer.parseInt(vars[2])) + ","
									+ Integer.toString(Integer.parseInt(vars[0]) - Integer.parseInt(keys[2]))));
					context.write(new Text("Q5a"),
							new Text(keys[1] + "," 
									+ Float.toString(Integer.parseInt(vars[1]) / Integer.parseInt(vars[2])) + ","
									+ Integer.toString(Integer.parseInt(vars[0]) - Integer.parseInt(keys[2]))));
				}
				break;
			case "City":
				context.write(new Text("Q6,City"),
						new Text(keys[1] + "," + Integer.toString(Integer.parseInt(vars[0]))));
				break;
			
			case "Q7":
				context.write(new Text(keys[0] + "," + keys[1]), new Text(vars[0]+","+vars[1]));
				break;
			}
		}
	}

}
