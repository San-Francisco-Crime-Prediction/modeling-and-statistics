package statistics.categoriesbytimeunit.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopKCategoriesMapper extends Mapper<Text, Text, Text, LongWritable> {

    private TreeMap<Long, String> treeMap;

    @Override
    protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        treeMap = new TreeMap<>();
    }

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        // (assault 37)
        // arson 24
        // Read from statistics.categorycount job
        String category = value.toString().split(" ")[0];
        Long count = Long.valueOf(value.toString().split(" ")[1]);
        treeMap.put(count, category);
        if (treeMap.size() > 10)
            treeMap.remove(treeMap.firstKey());
    }

    @Override
    protected void cleanup(Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : treeMap.entrySet()) {

            long count = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(name), new LongWritable(count));
        }
    }
}
