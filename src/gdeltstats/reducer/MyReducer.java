package gdeltstats.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String,Integer> map = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        for(IntWritable i : values){
            map.merge(key.toString(),i.get(),Integer::sum);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        for(Map.Entry<String,Integer> e: map.entrySet()){
            context.write(new Text(e.getKey()),new IntWritable(e.getValue()));
        }
    }
}
