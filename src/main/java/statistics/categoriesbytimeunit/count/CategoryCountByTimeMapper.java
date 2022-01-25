package statistics.categoriesbytimeunit.count;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.ArrayList;

public class CategoryCountByTimeMapper extends Mapper<Text, Text, Text, NullWritable> {

    private ArrayList<String> topKCategories;

    @Override
    protected void setup(Mapper<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        topKCategories = new ArrayList<>();
        File file = new File(context.getConfiguration().get("top_k_output_path"));
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);

        String line;
        while ((line = br.readLine()) != null)
            topKCategories.add(line.split(" ")[0]);
        fr.close();
    }

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // I only write top-k categories
        String[] tokens = value.toString().split(",");
        for (String c: topKCategories)
            if (c.equals(tokens[1]))
                context.write(new Text(tokens[1]
                        + Integer.parseInt(context.getConfiguration().get("feature_distribution_index"))), null);
    }

}
