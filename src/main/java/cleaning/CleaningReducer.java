package cleaning;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CleaningReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

        float sumOfAllX = 0;
        float sumOfAllY = 0;
        int numberOfAllX = 0;
        int numberOfAllY = 0;
        ArrayList<String[]> tuples = new ArrayList<>();

        for (Text t : values) {

            String[] fields = t.toString().split(",");
            tuples.add(fields);

            try {
                sumOfAllX += Float.parseFloat(fields[7]);
                numberOfAllX += 1;
                sumOfAllY += Float.parseFloat(fields[8]);
                numberOfAllY += 1;
            } catch (NumberFormatException e) {
                // Ignore this row during the computation of the mean
            }

        }

        // Check if there is at least a value with a valid Longitude
        if (numberOfAllX > 0)
            sumOfAllX = sumOfAllX / numberOfAllX;

        // Check if there is at least a value with a valid Latitude
        if (numberOfAllY > 0)
            sumOfAllY = sumOfAllY / numberOfAllY;

        for (String[] tuple : tuples) {

            // If there is a correct mean of all computable Longitudes and Latitudes
            if(numberOfAllX > 0 && numberOfAllY > 0) {
                try {
                    if ((Float.parseFloat(tuple[8]) >= 90 && Float.parseFloat(tuple[8]) < 93)) {
                        tuple[8] = Float.toString(sumOfAllY);
                        tuple[7] = Float.toString(sumOfAllX);
                    }
                } catch (NumberFormatException e) {
                    tuple[8] = Float.toString(sumOfAllY);
                    tuple[7] = Float.toString(sumOfAllX);
                }
            }

            context.write(null, new Text(String.join(",", tuple)));
        }
    }
}


