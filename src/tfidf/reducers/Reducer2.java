package tfidf.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer2 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int highest = Integer.MIN_VALUE;
		Iterator<Text> it = values.iterator();
		ArrayList<String> list = new ArrayList<>();
		for(Text t: values) {
			String[] temp = t.toString().split("\t");
			String unigram = temp[0];
			String frequency = temp[1];
			String all = unigram + "\t" + frequency;
			list.add(all);
			int curr = Integer.parseInt(temp[1]);
			if(highest < curr) {
				highest = curr;
			}
		}
		for(String t: list) {
			String[] ts = t.split("\t");
			String unigram = ts[0];
			String frequency = ts[1];
			double TFValue = 0.5 + 0.5 * (Integer.parseInt(frequency)/highest);
			
			context.write(key, new Text(unigram + "\t" + frequency + "\t" + TFValue));
		}
	}
}
