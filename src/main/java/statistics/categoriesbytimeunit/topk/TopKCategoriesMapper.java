package statistics.categoriesbytimeunit.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopKCategoriesMapper extends Mapper<LongWritable, Text, Text, Text> {

    private TreeMap<Long, String> treeMap;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {
        treeMap = new TreeMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) {

        // Read from statistics.featurecount job a tuple of form "ASSAULT 5245"
        String category = value.toString().split(";")[0];
        Long count = Long.valueOf(value.toString().split(";")[1].replaceAll("\\s",""));
        treeMap.put(count, category);
        if (treeMap.size() > 10)
            treeMap.remove(treeMap.firstKey());
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : treeMap.entrySet()) {

            Long count = entry.getKey();
            String name = entry.getValue();

            context.write(new Text(""), new Text(name + "," + count.toString()));
        }
    }
}
