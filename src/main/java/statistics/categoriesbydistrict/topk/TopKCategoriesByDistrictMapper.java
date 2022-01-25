package statistics.categoriesbydistrict.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TopKCategoriesByDistrictMapper extends Mapper<Text, Text, Text, Text> {


    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // input (K: distretto, tipoCrimine, V: numOccorrenze)
        // output: (K: tipo V: distretto, numOccorrenze)
        String[] tokens = key.toString().split(" ");
        String district = tokens[0];
        String category = tokens[1];

        context.write(new Text(category), new Text(district + " " + value));
    }
}
