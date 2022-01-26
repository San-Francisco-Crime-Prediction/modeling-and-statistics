package statistics.topk.bytimeunit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FeatureCountByParameterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int categoryByUnitTimeCount = 0;
        for (IntWritable v : values)
            categoryByUnitTimeCount += v.get();

        context.write(key, new IntWritable(categoryByUnitTimeCount));
    }
}
