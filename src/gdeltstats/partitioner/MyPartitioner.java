package gdeltstats.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text key, NullWritable value, int numReduceTasks) {

        return 0;
    }
}
