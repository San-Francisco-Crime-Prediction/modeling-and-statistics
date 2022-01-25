package statistics.categoriesbydistrict.topk;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopKCategoriesByDistrictReducer extends Reducer<Text, Text, Text, LongWritable> {

    // input: (K: tipo V: distretto, numOccorrenze)
    // output: (K: tipo, distretto V: numOccorrenze)

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        TreeMap<Long, String> treeMap = new TreeMap<>();

        for (Text v : values) {
            String[] tokens = v.toString().split(" ");
            treeMap.put(Long.valueOf(tokens[1]), tokens[0]);

            if (treeMap.size() > 5)
                treeMap.remove(treeMap.firstKey());

        }

        for (Map.Entry<Long, String> entry : treeMap.entrySet())
            context.write(new Text(key + " " + entry.getValue()), new LongWritable(entry.getKey()));
    }

}
