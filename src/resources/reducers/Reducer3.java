package resources.reducers;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		ArrayList<String> list = new ArrayList<>();
		for (Text val : values) {
			String[] v = val.toString().split("\t");
			String id = v[0];
			String TFvalue = v[1];
			sum +=1;
			list.add(id+"\t"+TFvalue);
			
		}
		for(String t: list) {
			String[] value = t.split("\t");
			String id = value[0];
			String TFvalue = value[1];
			context.write(key, new Text(id + "\t" + TFvalue + "\t" + sum));
		}
	}
}
