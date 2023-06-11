package HotelBooking;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class HotelBookingDriver {
    public static void main(String[] args) {
        JobClient my_client1 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf1 = new JobConf(HotelBookingDriver.class);

        // Set a name of the Job
        job_conf1.setJobName("RevenuePerMonth");

        // Specify data type of output key and value
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf1.setMapperClass(HotelBooking.HotelBookingMapper.class);
        job_conf1.setReducerClass(HotelBooking.HotelBookingReducer.class);

        // Specify formats of the data type of Input and output
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        // arg[0] = name of input directory on HDFS, and arg[1] = name of output
        // directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileInputFormat.setInputPaths(job_conf1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[2]));

        my_client1.setConf(job_conf1);
        try {
            // Run the job
            JobClient.runJob(job_conf1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}