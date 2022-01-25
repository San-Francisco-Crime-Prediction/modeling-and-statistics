package statistics.categoriesbytimeunit.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CategoryCountByTimeReducer extends Reducer<Text, NullWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int categoryByUnitTimeCount = 0;
        for (NullWritable v: values)
            categoryByUnitTimeCount++;

        context.write(key, new IntWritable(categoryByUnitTimeCount));
    }
}
