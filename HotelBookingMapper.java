package HotelBooking;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.DoubleAccumulator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class HotelBookingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    // private final static DoubleWritable revenue = new DoubleWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {

        String inputValue_string = value.toString();
        String[] hotel_booking_data = inputValue_string.split(",");

        // Check if the booking is within the specified time period (2015-2016)
        // double arrivalYear = Double.parseDouble(hotel_booking_data[3]);
        String arrivalYearStr = hotel_booking_data[3];
        String bookingStatusStr = hotel_booking_data[1];

        if (!isNumeric(arrivalYearStr) || !isNumeric(bookingStatusStr)) {
            // Print the problematic values
            System.err.println("Invalid numeric value: arrivalYearStr = " + arrivalYearStr + ", bookingStatusStr = "
                    + bookingStatusStr);
            // Skip this record if the values are not numeric
            return;
        }

        double arrivalYear = Double.parseDouble(arrivalYearStr);
        double bookingStatus = Double.parseDouble(bookingStatusStr);

        if (bookingStatus == 0) {
            // Extract the arrival month and revenue from the dataset
            String arrivalMonth = hotel_booking_data[4];
            // DecimalFormat decimalFormat = new DecimalFormat("#0.0");
            // String singleDecimal = decimalFormat.format(hotel_booking_data[11]);
            double avgPricePerRoom = Double.parseDouble(hotel_booking_data[11]);

            double stay_weekdays = Double.parseDouble(hotel_booking_data[7]);
            double stay_weekends = Double.parseDouble(hotel_booking_data[8]);

            // Calculate the revenue for this booking
            double totalRevenue = avgPricePerRoom * (stay_weekdays + stay_weekends);

            // Emit the arrival month as the key and revenue as the value
            output.collect(new Text(arrivalMonth), new DoubleWritable(totalRevenue));
        }
    }

    private boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");
    }
}