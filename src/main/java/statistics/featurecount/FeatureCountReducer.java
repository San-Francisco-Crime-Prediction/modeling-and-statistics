package statistics.featurecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FeatureCountReducer extends Reducer<Text, NullWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int categoryCount = 0;
        for (NullWritable v: values) {
            categoryCount++;
        }
        context.write(key, new IntWritable(categoryCount));
    }
}
