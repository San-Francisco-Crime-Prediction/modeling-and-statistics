package statistics.categoriesbydistrict.count;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CategoryByDistrictCountMapper extends Mapper<Text, Text, Text, NullWritable> {


    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        context.write(new Text(tokens[1] + tokens[4]), null);
    }

}
