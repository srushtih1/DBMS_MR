package CustomerReservation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CustomerReservationDriver {
    public static void main(String[] args) {
        JobClient my_client2 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf2 = new JobConf(CustomerReservationDriver.class);

        // Set a name of the Job
        job_conf2.setJobName("RevenuePerMonthCR");

        // Specify data type of output key and value
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf2.setMapperClass(CustomerReservation.CustomerReservationMapper.class);
        job_conf2.setReducerClass(CustomerReservation.CustomerReservationReducer.class);

        // Specify formats of the data type of Input and output
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        // arg[0] = name of input directory on HDFS, and arg[1] = name of output
        // directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[1]));

        my_client2.setConf(job_conf2);
        try {
            // Run the job
            JobClient.runJob(job_conf2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}