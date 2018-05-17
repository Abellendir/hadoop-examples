package airportstats.reducers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		switch (key.toString().split(",")[0]) {
		case "Q1A2a":
		case "Q1A2b":
		case "Q1A2c":
			questionQ1a(key, values, context);
			;
			break;
		case "Q3":
			questionQ3(key, values, context);
			break;
		case "Q4":
			questionQ4(key, values, context);
			break;
		case "Q5":
			questionQ5(key, values, context);
			break;
		case "Q6":
			questionQ6(key, values, context);
			break;
		case "Q7":
			questionQ7(key, values, context);
			break;
		case "Q5a":
			questionQ5a(key, values, context);
			break;
		default:
			break;
		}
	}
	
	private void questionQ5a(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		context.write(new Text("Q5"), new Text("alternative"));
		int age = 0;
		float oldDelay = 0;
		float oldc = 0;
		float newDelay = 0;
		float newc = 0;
		for(Text t: values){
			String vals[] = t.toString().split(",");
			age = Integer.parseInt(vals[vals.length-1]);
			if(age >= 20){
				oldDelay += Float.parseFloat(vals[1]);
				oldc++;
			}else{
				newDelay += Float.parseFloat(vals[1]);
				newc++;
			}
		}
		context.write(new Text("Old"), new Text("Average" + "," + (oldDelay/oldc)));
		context.write(new Text("New"), new Text("Average" + "," + (newDelay/newc)));
		
	}
	
	private void questionQ7(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		context.write(new Text("Q7"), new Text(key));
		Map<String, Integer> map = new HashMap<>();
		for(Text t: values) {
			map.put(t.toString().split(",")[0], Integer.parseInt(t.toString().split(",")[1]));
		}
		Stream<Map.Entry<String, Integer>> sorted = map.entrySet().stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		Iterator<Entry<String, Integer>> it = sorted.iterator();
		int count = 0;
		while (it.hasNext()) {
			//if (count == 10) break;
			Entry<String, Integer> e = it.next();
			context.write(new Text(key.toString().split(",")[0] + "," + key.toString().split(",")[1] + "," + e.getKey()), new Text(Integer.toString(e.getValue())));
			count++;
		}
		context.write(new Text(key.toString().split(",")[1] + ",total destinations"), new Text(Integer.toString(count))); 
	}

	private void questionQ6(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(new Text("Q6"), new Text("City," + "Delay"));
		Map<String, Integer> map = new HashMap<>();
		for (Text t : values) {
			map.put(t.toString().split(",")[0], Integer.parseInt(t.toString().split(",")[1]));
		}
		Stream<Map.Entry<String, Integer>> sorted = map.entrySet().stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		Iterator<Entry<String, Integer>> it = sorted.iterator();
		int count = 0;
		while (it.hasNext()) {
			if (count == 10)
				break;
			Entry<String, Integer> e = it.next();
			context.write(new Text(e.getKey()), new Text(Integer.toString(e.getValue())));
			count++;
		}
	}

	private void questionQ5(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(new Text("Year"), key);
		Map<String, Float> map = new HashMap<>();
		for (Text t : values) {
			map.put(key.toString().split(",")[1] + "," + t.toString().split(",")[0] + "," + t.toString().split(",")[2],
					Float.parseFloat(t.toString().split(",")[1]));
			// context.write();
		}
		Stream<Map.Entry<String, Float>> sorted = map.entrySet().stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		Iterator<Entry<String, Float>> it = sorted.iterator();
		int count = 0;
		while (it.hasNext()) {
			if (count == 10)
				break;
			Entry<String, Float> e = it.next();
			context.write(new Text(e.getKey()), new Text(Float.toString(e.getValue())));
			count++;
		}

	}

	private void questionQ4(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text t : values) {
			context.write(key, t);
		}

	}

	private void questionQ3(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(new Text("Year"), key);
		Map<String, Integer> map = new HashMap<>();
		for (Text t : values) {
			map.put(key.toString().split(",")[1] + "," + t.toString().split(",")[0],
					Integer.parseInt(t.toString().split(",")[1]));
			// context.write();
		}
		Stream<Map.Entry<String, Integer>> sorted = map.entrySet().stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		Iterator<Entry<String, Integer>> it = sorted.iterator();
		int count = 0;
		while (it.hasNext()) {
			if (count == 10)
				break;
			Entry<String, Integer> e = it.next();
			context.write(new Text(e.getKey()), new Text(Integer.toString(e.getValue())));
			count++;
		}

	}

	private void questionQ1a(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text("Label"), new Text("Delay"));
		Map<String, Float> map = new HashMap<>();
		for (Text t : values) {
			map.put(t.toString().split(",")[0], Float.parseFloat(t.toString().split(",")[1]));
		}
		Stream<Map.Entry<String, Float>> sorted = map.entrySet().stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		Iterator<Entry<String, Float>> it = sorted.iterator();

		while (it.hasNext()) {
			Entry<String, Float> e = it.next();
			context.write(new Text(e.getKey()), new Text(Float.toString(e.getValue())));

		}
	}
}
