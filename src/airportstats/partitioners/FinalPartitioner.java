package airportstats.partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FinalPartitioner extends  Partitioner<Text, Text>  {

	@Override
	public int getPartition(Text arg0, Text arg1, int arg2) {
		switch(arg0.toString().split(",")[0]) {
		case "Q1A2a": return 0;
		case "Q1A2b": return 1;
		case "Q1A2c": return 2;
		case "Q3": return 3;
		case "Q4": return 4;
		case "Q5": case "Q5a":return 5;
		case "Q6": return 6;
		case "Q7": return 7;
		default: return 0;
		}
	}

}
