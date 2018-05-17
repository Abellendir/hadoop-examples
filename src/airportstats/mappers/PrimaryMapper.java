package airportstats.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import airportstats.resources.Day;
import airportstats.resources.Month;

public class PrimaryMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		if (!values.toString().isEmpty()) {
			String columns[] = values.toString().split(",");
			if (!columns[0].equalsIgnoreCase("year")) {
				if (!columns[15].equals("") && !columns[15].equalsIgnoreCase("NA")) {
					if (!columns[3].equals("NA") && !columns[15].equals("NA") && Integer.parseInt(columns[15]) != 0) {// day
						context.write(new Text("Day" + "," + Day.values()[Integer.parseInt(columns[3]) - 1].toString()),
								new Text(columns[15] + "," + "1"));// Q1/Q2
						/* Output: "Day<tab>Day, <delay,1>" */
					}
					
					if (!columns[1].equals("NA") && !columns[15].equals("NA") && Integer.parseInt(columns[15]) != 0) {// Month
						context.write(
								new Text("Month" + "," + Month.values()[Integer.parseInt(columns[1]) - 1].toString()),
								new Text(columns[15] + "," + "1"));// Q1/Q2
						/* Output: "Month<tab>Month, <delay,1>" */
					}
					
					if (!columns[5].equals("NA") && !columns[15].equals("NA") && Integer.parseInt(columns[15]) != 0) {// Time
						context.write(new Text("Time" + "," + Integer.parseInt(columns[5]) % 24),
								new Text(columns[15] + "," + "1"));// Q1/Q2
						/* Output: "Time<tab>Time, <delay,1>" */
					}
					
					/**
					 * Q3 destination
					 */
					if (!columns[17].equals("NA")  && !columns[0].equals("")  && !columns[0].equals("") && !columns[0].equals("NA") ) {// Busiest airport counts occurence
						context.write(new Text("Airport" + "," + columns[17]), new Text(columns[0] + "," + "1"));// Q3

						/* Output: "Airport<Tab>AirportCode, <Year,1>" */
					}
					
					/**
					 * Q3 orgin
					 */
					if (!columns[16].equals("NA")  && !columns[0].equals("")  && !columns[0].equals("") && !columns[0].equals("NA")) {
						context.write(new Text("Airport" + "," + columns[16]), new Text(columns[0] + "," + "1"));
					}

					
					/**
					 * Q4
					 */
					if (!columns[8].equals("NA") && !columns[24].equals("NA") && Integer.parseInt(columns[24]) != 0) {// Carrier
						context.write(new Text("Carrier" + "," + columns[8]), new Text(columns[24] + "," + "1"));// Q4
						/* Output: "Carrier<Tab>carrierCode, <carrierDelay,1>" */
					}
					
					/**
					 * Q5
					 */
					if (!columns[10].equals("NA") && !columns[0].equals("") && !columns[10].equals("") && !columns[15].equals("NA") && !columns[14].equals("NA")
							&& !(Integer.parseInt(columns[15]) != 0 && Integer.parseInt(columns[14])!=0)  && !columns[0].equals("") && !columns[0].equals("NA")) {// Older plane delay
						context.write(new Text("Plane" + "," + columns[10]), new Text(
								columns[0] + "," + (Integer.parseInt(columns[15]) + Integer.parseInt(columns[14]))));// Q5
						/* Output: "Plane<Tab>tailnum, <Year, delay>" */
					}
					
					/**
					 * Q6
					 */
					if (!columns[25].equals("NA") && Integer.parseInt(columns[25]) != 0 && !columns[16].equals("NA")) {// Weather
																														// delay
						context.write(new Text("Weather Airport" + "," + columns[16]),
								new Text((Integer.parseInt(columns[25])) + "," + "1"));// Q6
						/* Output: "Weather Airport<Tab>AirportCode, <weatherDelay,1>" */
					}
					
					/**
					 * Q7
					 */
					if (!columns[17].equals("NA") && !columns[16].equals("NA")) {
						context.write(new Text("Q7," + columns[16]), new Text(columns[17] + ",1"));
					}
				}
			}
		}
	}

}
