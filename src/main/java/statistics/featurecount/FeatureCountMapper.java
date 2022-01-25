package statistics.featurecount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FeatureCountMapper extends Mapper<Text, Text, Text, NullWritable> {

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        int featureIndex = Integer.parseInt(context.getConfiguration().get("feature_count_index"));
        String category = value.toString().split(",")[featureIndex];
        context.write(new Text(category), null);
    }
}
