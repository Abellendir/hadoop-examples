package gdeltstats.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Primary mapper for GDELT Data
 */
public class Primary extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @Discription TODO
     */

    //Date          0  keep
    //Source        1  keep
    //Target        2  keep
    //CAMEOCode     3  keep
    //NumEvents     4  keep
    //NumArts       5  keep
    //QuadClass     6  ?
    //Goldstein     7  ?
    //SourceGeoType 8  ?
    //SourceGeoLat  9  ?
    //SourceGeoLong 10 ?
    //TargetGeoType 11 ?
    //TargetGeoLat  12 ?
    //TargetGeoLong 13 ?
    //ActionGeoType 14 ?
    //ActionGeoLat  15 ?
    //ActionGeoLong 16 ?
    private Map<String,Integer> map = new HashMap<>();
    private Map<String,Integer> map2 = new HashMap<>();

    public void map(LongWritable key, Text value, Context context)  {
        if (!value.toString().isEmpty()) { // removes empty strings
            String data[] = value.toString().split("\t");

            if (!data[0].equals("Date")) {
                String source = data[1];
                String CAMEOCode = data[3];
                int numEvents = Integer.parseInt(data[4]);
                map.merge(source + "\t" + CAMEOCode, numEvents, Integer::sum);
                //map2.merge(source + "\t" + CAMEOCode + "\tArticals", Integer.parseInt(data[5]), Integer::sum );
            }
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        for(Map.Entry<String,Integer> e: map.entrySet()){
            context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
        }
    }
}//End Class
