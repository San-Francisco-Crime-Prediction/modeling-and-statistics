package statistics.categoriesbytimeunit.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import statistics.featurecount.FeatureCountMapper;
import statistics.featurecount.FeatureCountReducer;

import java.io.IOException;

public class TopKCategoriesJob {


    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job j = Job.getInstance(conf, "Top-k feature counting");
            j.setJarByClass(TopKCategoriesJob.class);

            j.setMapperClass(TopKCategoriesMapper.class);
            j.setReducerClass(TopKCategoriesReducer.class);
            j.setMapOutputKeyClass(Text.class);
            j.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(j, new Path("count-statistic"));
            FileOutputFormat.setOutputPath(j, new Path("top-k-categories"));
            j.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
