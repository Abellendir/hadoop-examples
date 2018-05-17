package tfidf.partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GeneralPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReducers) {
		//System.out.println(key.toString() + "\t" + value.toString());
		return Math.abs(key.hashCode() % numReducers);
	}

}
