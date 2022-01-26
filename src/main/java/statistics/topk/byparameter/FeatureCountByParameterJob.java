package statistics.topk.byparameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FeatureCountByParameterJob {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("top_k_output_path", "hdfs://master:9000/user/hadoopa/top-k_out/part-r-00000");
        conf.set("parameter_col_index", "3");
        conf.set("feature_col_index", "1");
        try {
            Job j = Job.getInstance(conf, "Distribution of top-k categories over time");
            j.setJarByClass(FeatureCountByParameterJob.class);

            j.setMapperClass(FeatureCountByParameterMapper.class);
            j.setReducerClass(FeatureCountByParameterReducer.class);
            j.setMapOutputKeyClass(Text.class);
            j.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(j, new Path("cleandata"));
            FileOutputFormat.setOutputPath(j, new Path("feature-count-by-time_out"));
            j.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
