package gdeltstats.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class SearchMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    Map<String,Integer> map = new HashMap<>();

    public void map(LongWritable key, Text value, Context context){
        Configuration conf = context.getConfiguration();
        String param = conf.get("search");

        if (!value.toString().isEmpty()){
            String data[] = value.toString().split("\t");
            if(param.length() == 2){
                if(param.equals(data[1].substring(0,2))) {
                    map.put(data[1] + "\t" + data[0], Integer.parseInt(data[2]));
                }
            }else{
                if(param.equals(data[1])){
                    map.put(data[1] + "\t" + data[0], Integer.parseInt(data[2]));
                }
            }
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException{

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
