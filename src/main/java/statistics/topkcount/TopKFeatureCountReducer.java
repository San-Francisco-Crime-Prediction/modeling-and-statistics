package statistics.topkcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopKFeatureCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, String> treeMap;

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) {
        treeMap = new TreeMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) {
        int categoryCount = 0;
        for (IntWritable v: values) {
            categoryCount += v.get();
        }
        treeMap.put(categoryCount, key.toString());
        if (treeMap.size() > Integer.parseInt(context.getConfiguration().get("k")))
            treeMap.remove(treeMap.firstKey());

    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> entry : treeMap.entrySet()) {

            Integer count = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(name + ";"), new IntWritable(count));
        }
    }
}
