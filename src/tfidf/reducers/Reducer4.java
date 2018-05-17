package tfidf.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer4 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> v = values.iterator();
		while(v.hasNext()){
			context.write(key, v.next());
		}
	}
}
