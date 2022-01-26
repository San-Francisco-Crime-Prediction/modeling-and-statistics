package statistics.topk.byparameter;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class FeatureCountByParameterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private ArrayList<String> topKCategories;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException {
        topKCategories = new ArrayList<>();
        Path pt = new Path(context.getConfiguration().get("top_k_output_path"));

        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line = br.readLine();
        while (line != null) {
            topKCategories.add(line.split(";")[0]);
            line = br.readLine();
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // I only write top-k categories
        String[] tokens = Iterables.toArray(Splitter.on(Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).
                        split(value.toString()),
                String.class);

        for (String c : topKCategories)
            if (c.equals(tokens[1]))
                context.write(new Text(tokens[Integer.parseInt(context.getConfiguration().get("feature_col_index"))]
                        + " "
                        + tokens[Integer.parseInt(context.getConfiguration().get("parameter_col_index"))]), new IntWritable(1));
    }

}
