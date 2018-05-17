package airportstats.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParserReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		switch (key.toString().split(",")[0]) {
		case "Day":
		case "Month":
		case "Time":
			question1A2(key, values, context);
			break;
		case "Airport":
			question3(key, values, context);
			break;
		case "Carrier":
			question4(key, values, context);
			break;
		case "Plane":
			question5(key, values, context);
			break;
		case "Weather Airport":
			question6(key, values, context);
			break;
		case "Q7":
			question7(key, values, context);
			break;
		default:
			break;
		}
	}

	/**
	 * Q7
	 * 
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void question7(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<String,Integer> map = new HashMap<>();
		String airport = key.toString().split(",")[1];
		Text k = new Text();
		Text v = new Text();
		for(Text t: values) {
			String s[] = t.toString().split(",");
			if(s[0].equals("AirportName")) {
				airport = s[1];
			}else {
				map.merge(s[0], Integer.parseInt(s[1]), Integer::sum);
			}
		}
		for(Map.Entry<String, Integer> e: map.entrySet()) {
			k.set("Q7" + "," + airport);
			v.set(e.getKey() + "," + e.getValue());
			context.write(k, v);
		}
	}

	/*
	 * Weather delay output:
	 */
	private void question6(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		String city = "";
		for (Text t : values) {
			String[] vals = t.toString().split(",");
			if (vals[0].equals("City")) {
				city = vals[1];
			} else {
				sum += Integer.parseInt(vals[0]);
				count += Integer.parseInt(vals[1]);
			}
		}
		if (sum != 0) {
			context.write(new Text("City" + "," + city), new Text(Integer.toString(count)));
		}
	}

	/*
	 * Older plane delay output:
	 */
	private void question5(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum[] = new int[22];
		int count[] = new int[22];
		String year = "";
		String tailNumber = key.toString().split(",")[1];
		for (Text t : values) {
			String vals[] = t.toString().split(",");
			if (vals[0].equals("Year")) {
				year = vals[1];
			} else {
				int y = Integer.parseInt(vals[0]);
				sum[y - 1987] += Integer.parseInt(vals[1]);
				count[y - 1987] += 1;
			}
		}
		int currYear = 1987;
		int cn = 0;
		for (int c : sum) {
			if (!year.equals("") && currYear >= Integer.parseInt(year)) {
				context.write(new Text("Plane" + "," + tailNumber + "," + year), new Text(
						Integer.toString(currYear) + "," + Integer.toString(c) + "," + Integer.toString(count[cn])));
			}
			currYear += 1;
			cn += 1;
		}

	}

	/*
	 * Carrier Delay output:
	 */
	private void question4(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		String carrierName = "";
		for (Text t : values) {
			String vals[] = t.toString().split(",");
			if (vals[0].equals("CarrierName")) {
				carrierName = vals[1];
			} else {
				sum += Integer.parseInt(vals[0]);
				count += Integer.parseInt(vals[1]);
			}
		}
		if (sum != 0)
			context.write(new Text("Carrier" + "," + carrierName),
					new Text(Integer.toString(sum) + "," + Integer.toString(count)));
	}

	/*
	 * Busiest Airport output:
	 * Airport<Tab>Airportname<Tab>Year<,>Total<Tab>...<Tab>Year<,>Total
	 */
	private void question3(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String airportName = "";
		int[] counts = new int[22];
		for (Text t : values) {
			String[] s = t.toString().split(",");
			if (s[0].equals("AirportName")) {
				airportName = t.toString().split(",")[1];
			} else if (s.length > 1) {
				int total = Integer.parseInt(s[1]);
				int year = Integer.parseInt(s[0]);
				counts[year - 1987] += total;
			}
		}
		int year = 1987; // initializes year to 1987 to correspond with the result in the count array
		for (int c : counts) {
			if (c != 0) {
				context.write(new Text(key.toString().split(",")[0] + "," + airportName),
						new Text(Integer.toString(year) + "," + Integer.toString(c)));
			}
			year += 1;
		}
	}

	/**
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 *             outputs: Day DayOfWeak<Tab>Total<Tab>Count Month
	 *             MonthOfYear<Tab>Total<Tab>Count Time Hour<Tab>Total<Tab>Count
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