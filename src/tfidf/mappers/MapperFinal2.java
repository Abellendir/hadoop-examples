package tfidf.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFinal2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!value.toString().isEmpty()) {
			String id = value.toString().split("<====>")[1];
			if (!value.toString().split("<====>")[2].equals("")) {
				String s = value.toString().split("<====>")[2];
				String[] s1 = s.split("\\.");
				for (String s2 : s1) {
					if (!s2.equals(""))
						context.write(new Text(id), new Text("sentence<====>" + s2));
				}
			}
		}
	}
}
