package statistics.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
*
* OK 1. Quante volte occorre ciascun crimine
* OK 2. Quante volte si verifica un crimine per ciascun distretto
* OK 3. Estrarre i top-k crimini più frequenti in assoluto, per plottarne la distribuzione in giorni/mesi
* OK 4. Estrarre i top-k distretti più criminosi in assoluto, per plottarne la distribuzione in anni
* 5. Quali sono e quante volte occorrono i top-k crimini per ciascun distretto
*
* */

public class TopKFeatureCountJob {

    public static final String CATEGORY_COLUMN_INDEX = "1";
    public static final String DISTRICT_COLUMN_INDEX = "4";


    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("feature_index", CATEGORY_COLUMN_INDEX);
        conf.set("k", "10"); // Set to a high value if needed
        try {
            Job j = Job.getInstance(conf, "Feature counting");
            j.setJarByClass(TopKFeatureCountJob.class);

            j.setMapperClass(TopKFeatureCountMapper.class);
            j.setReducerClass(TopKFeatureCountReducer.class);
            j.setMapOutputKeyClass(Text.class);
            j.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(j, new Path("cleandata"));
            FileOutputFormat.setOutputPath(j, new Path("top-k_out"));
            j.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
