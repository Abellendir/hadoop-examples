package airportstats.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCombiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		switch (key.toString().split(",")[0]) {
		case "Day":
		case "Month":
		case "Time":
			dualCombine(key, values, context,"NONE");
			break;
		case "Carrier":
			dualCombine(key, values, context,"CarrierName");
			break;
		case "Plane":
			for(Text t: values){
				context.write(key,t);
			}
			break;
		case "Weather Airport":
			dualCombine(key,values,context,"City");
			break;
		case "Airport":
		case "Q7":
			genericCombine2(key, values, context,"AirportName");
			break;
		default:
			break;
		}
	}

	/**
	 * @param key
	 * @param values
	 * @param context
	 * @param label
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void genericCombine(Text key, Iterable<Text> values, Context context, String label)
			throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<>();
		for (Text t : values) {
			String[] s = t.toString().split(",");
			if (!s[0].equals(label)) {
				map.merge(s[0], Integer.parseInt(s[1]), Integer::sum);
			}else context.write(key, t);
		}
		for(Map.Entry<String, Integer> e : map.entrySet()) {
			context.write(key, new Text(e.getKey() + "," + e.getValue()));
		}
	}
	/**
	 * @param key
	 * @param values
	 * @param context
	 * @param label
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void genericCombine2(Text key, Iterable<Text> values, Context context, String label)
			throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<>();
		for (Text t : values) {
			String[] s = t.toString().split(",");
			if (!s[0].equals(label)) {
				map.merge(s[0], Integer.parseInt(s[1]), Integer::sum);
			}else context.write(key, t);
		}
		for(Map.Entry<String, Integer> e : map.entrySet()) {
			context.write(key, new Text(e.getKey() + "," + e.getValue()));
		}
	}

	/**
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @deprecated
	 */
	private void q7(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<String, Integer> map = new HashMap<>();
		for (Text t : values) {
			String[] s = t.toString().split(",");
			if (!s[0].equals("AirportName")) {
				map.merge(s[0], Integer.parseInt(s[s.length-1]), Integer::sum);
			}else context.write(key, t);
		}
		for(Map.Entry<String, Integer> e : map.entrySet()) {
			context.write(key, new Text(e.getKey() + "," + e.getValue()));
		}
	}
	
	private void dualCombine(Text key, Iterable<Text> values, Context context, String label) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for(Text t: values) {
			String s[] = t.toString().split(",");
			if(!s[0].equals(label)) {
				sum += Integer.parseInt(s[0]);
				count += Integer.parseInt(s[1]);
			}else context.write(key, t);
		}
		context.write(key, new Text(Integer.toString(sum) + "," + count));
	}
	
	/**
	 * @deprecated
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void question1A2(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for (Text val : values) {
			String s[] = val.toString().split(",");
			sum += Integer.parseInt(s[0]);
			count += Integer.parseInt(s[1]);
		}
		context.write(key, new Text(Integer.toString(sum) + "," + count));

	}
}
