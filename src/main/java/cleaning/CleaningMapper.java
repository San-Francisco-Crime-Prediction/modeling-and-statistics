package cleaning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.Locale;

public class CleaningMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        if (fields.length == 9) {

            // Header check: adding column names
            if (fields[0].equals("Dates")) {
                String newHeader = value + "TimeOfDay" + "Month" + "Year";
                context.write(new Text(""), new Text(newHeader));
            }
            else {
                boolean nullRow = false;
                String newAddress = "";
                String timeOfDay = "";
                String year = "";
                String month = "";
                for (String field : fields) {
                    // Null value removal
                    if (field.equals("")) {
                        nullRow = true;
                        break;
                    }
                    // Stopwords removal from address column
                    newAddress = fields[6].replace("av", "").
                            replace("st", "").
                            replace("of", "").
                            replace("block", "")
                            .replace("ln", "");

                    // Extracting time of day, month, and year from existing features
                    String date = fields[0].split(" ")[0];
                    String time = fields[0].split(" ")[1];

                    int hourOfDay = Integer.parseInt(time.split(":")[0]);
                    if (hourOfDay >= 5 && hourOfDay <= 11)
                        timeOfDay = "Morning";
                    else if (hourOfDay >= 12 && hourOfDay <= 17)
                        timeOfDay = "Afternoon";
                    else if (hourOfDay >= 18 && hourOfDay <= 21)
                        timeOfDay = "Evening";
                    else
                        timeOfDay = "Night";

                    year = date.split("-")[0];
                    month = Month.of(Integer.parseInt(date.split("-")[1])).
                            getDisplayName(TextStyle.FULL_STANDALONE, new Locale("en-US"));
                }

                StringBuilder finalRow = new StringBuilder();
                for (int i = 0; i < 6; i++) {
                    finalRow.append(fields[i]);
                }
                finalRow.append(newAddress).
                        append(fields[6]).
                        append(fields[7]).
                        append(timeOfDay).
                        append(month).append(year);

                if (!nullRow) {
                    context.write(new Text(""), new Text(finalRow.toString()));
                }
            }
        }
    }
}