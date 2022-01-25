package statistics.featurecount;

import cleaning.CleaningMapper;
import cleaning.CleaningReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FeatureCountJob {

    public static final String CATEGORY_COLUMN_INDEX = "1";
    public static final String DISTRICT_COLUMN_INDEX = "4";


    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("feature_index", CATEGORY_COLUMN_INDEX);
        try {
            Job j = Job.getInstance(conf, "Feature counting");
            j.setJarByClass(FeatureCountJob.class);

            j.setMapperClass(FeatureCountMapper.class);
            j.setReducerClass(FeatureCountReducer.class);
            j.setMapOutputKeyClass(Text.class);
            j.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(j, new Path("cleandata"));
            FileOutputFormat.setOutputPath(j, new Path("count-statistic"));
            j.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
