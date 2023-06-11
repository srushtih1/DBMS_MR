package CustomerReservation;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.DoubleAccumulator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CustomerReservationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        String inputValue_string = value.toString();
        String[] hotel_booking_data = inputValue_string.split(",");

        String bookingStatusStr = hotel_booking_data[9];

        if (bookingStatusStr.equals("Not_Canceled")) {
            String arrivalMonth = hotel_booking_data[5];
            double avgPricePerRoom = Double.parseDouble(hotel_booking_data[8]);

            double stay_weekdays = Double.parseDouble(hotel_booking_data[2]);
            double stay_weekends = Double.parseDouble(hotel_booking_data[1]);

            double totalRevenue = avgPricePerRoom * (stay_weekdays + stay_weekends);

            output.collect(new Text(arrivalMonth), new DoubleWritable(totalRevenue));
        }
    }
}