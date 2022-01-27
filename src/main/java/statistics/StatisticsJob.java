package statistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import statistics.byparametercount.FeatureCountByParameterMapper;
import statistics.byparametercount.FeatureCountByParameterReducer;
import statistics.topkcount.TopKFeatureCountMapper;
import statistics.topkcount.TopKFeatureCountReducer;

import java.io.IOException;

public class StatisticsJob {

    /*
     *
     * OK 1. Quante volte occorre ciascun crimine
     * OK 2. Quante volte si verifica un crimine per ciascun distretto
     * OK 3. Estrarre i top-k crimini più frequenti in assoluto, per plottarne la distribuzione in giorni/mesi
     * OK 4. Estrarre i top-k distretti più criminosi in assoluto, per plottarne la distribuzione in anni
     * OK 5. Quali sono e quante volte occorrono i top-k crimini per ciascun distretto
     * ...
     * */

    public static final String FIRST_JOB_OUTPUT_PATH = "hdfs://master:9000/user/hadoopa/top-k_out/part-r-00000";
    public static final String JOBS_INPUT_PATH = "hdfs://master:9000/user/hadoopa/cleandata";
    public static final String CATEGORY_COLUMN_INDEX = "1";
    public static final String DAYOFWEEK_COLUMN_INDEX = "3";
    public static final String DISTRICT_COLUMN_INDEX = "4";
    public static final String YEAR_COLUMN_INDEX = "11";

    public static void main(String[] args) {

        // From this main you can obtain top-k occurrences of the column given by featureIndex,
        // and also know their distribution against the column given by parameterIndex.

        // For example, to know top-k crimes per district, featureIndex = CATEGORY and parameterIndex = DISTRICT
        // Output of the first job will be in FIRST_JOB_OUTPUT_PATH, to know absolute numbers
        String featureIndex = CATEGORY_COLUMN_INDEX;
        String parameterIndex = DISTRICT_COLUMN_INDEX;
        String k = "50"; // For first job only

        Configuration confFirstJob = new Configuration();
        confFirstJob.set("feature_col_index", featureIndex);
        confFirstJob.set("k", k); // Set to a high value if needed
        Configuration confSecondJob = new Configuration();
        confSecondJob.set("top_k_output_path", FIRST_JOB_OUTPUT_PATH);
        confSecondJob.set("parameter_col_index", parameterIndex);
        confSecondJob.set("feature_col_index", featureIndex);

        try {
            Job firstJob = Job.getInstance(confFirstJob, "Feature counting");
            firstJob.setJarByClass(StatisticsJob.class);
            firstJob.setMapperClass(TopKFeatureCountMapper.class);
            firstJob.setReducerClass(TopKFeatureCountReducer.class);
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(firstJob, new Path(JOBS_INPUT_PATH));
            FileOutputFormat.setOutputPath(firstJob, new Path("top-k_out"));
            firstJob.waitForCompletion(true);

            Job secondJob = Job.getInstance(confSecondJob, "Distribution of top-k categories over parameter");
            secondJob.setJarByClass(StatisticsJob.class);
            secondJob.setMapperClass(FeatureCountByParameterMapper.class);
            secondJob.setReducerClass(FeatureCountByParameterReducer.class);
            secondJob.setMapOutputKeyClass(Text.class);
            secondJob.setMapOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(secondJob, new Path(JOBS_INPUT_PATH));
            FileOutputFormat.setOutputPath(secondJob, new Path("distribution_out"));
            secondJob.waitForCompletion(true);

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}