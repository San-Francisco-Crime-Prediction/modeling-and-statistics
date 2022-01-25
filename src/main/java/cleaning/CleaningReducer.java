package cleaning;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CleaningReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

        for (Text t : values) {
            context.write(null, t);
        }
    }
}


