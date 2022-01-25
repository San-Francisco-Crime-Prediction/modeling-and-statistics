package cleaning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CleaningJob {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        try {
            Job j = Job.getInstance(conf, "Filtering");
            j.setJarByClass(CleaningJob.class);

            j.setMapperClass(CleaningMapper.class);
            j.setReducerClass(CleaningReducer.class);
            j.setMapOutputKeyClass(Text.class);
            j.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(j, new Path("input"));
            FileOutputFormat.setOutputPath(j, new Path("cleandata"));
            j.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
