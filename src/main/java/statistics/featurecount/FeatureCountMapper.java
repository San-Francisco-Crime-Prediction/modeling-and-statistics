package statistics.featurecount;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class FeatureCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int featureIndex = Integer.parseInt(context.getConfiguration().get("feature_index"));

        String category = Iterables.toArray(Splitter.on(Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).
                        split(value.toString()),
                String.class)[featureIndex];

        context.write(new Text(category), new IntWritable(1));
    }
}
