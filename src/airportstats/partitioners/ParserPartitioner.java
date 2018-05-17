package airportstats.partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParserPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text arg0, Text arg1, int arg2) {
		System.out.println(arg0.toString() + "\t" + arg1.toString());
		return Math.abs(arg0.hashCode() % arg2);
	}

}
