package CustomerReservation;

import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CustomerReservationReducer extends MapReduceBase
        implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
            Reporter reporter) throws IOException {
        Text key = t_key;

        double totalRevenue = 0;
        while (values.hasNext()) {
            totalRevenue += values.next().get();
        }

        int monthNumber = Integer.parseInt(key.toString());
        String monthName = getMonthName(monthNumber);

        // output.collect(key, new DoubleWritable(totalRevenue));
        output.collect(new Text(monthName), new DoubleWritable(totalRevenue));
    }

    private String getMonthName(int monthNumber) {
        String[] monthNames = { "January", "February", "March", "April", "May", "June", "July", "August", "September",
                "October", "November", "December" };

        // Subtract 1 from the month number to match the array index
        int index = monthNumber - 1;

        // Check if the index is within the valid range
        if (index >= 0 && index < monthNames.length) {
            return monthNames[index];
        } else {
            return "Invalid Month";
        }
    }
}