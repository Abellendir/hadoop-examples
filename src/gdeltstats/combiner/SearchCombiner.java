package gdeltstats.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class SearchCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

    Map<String,Integer> map = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        for(IntWritable t: values){
            map.put(key.toString(),t.get());
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        Stream<Map.Entry<String, Integer>> sorted = map.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
        Iterator<Map.Entry<String, Integer>> it = sorted.iterator();
        int count = 0;
        while (it.hasNext()) {
            if (count == 10)
                break;
            Map.Entry<String, Integer> e = it.next();
            context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
            count++;
        }
    }

}
