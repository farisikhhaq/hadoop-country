package bigdatauas;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SalesCountryDriver {
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SalesCountryDriver.class);

        // Set a name of the Job
        job_conf.setJobName("Penjualan Tiap Negara");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // Menentukan tipe data dari kunci dan nilai keluaran
        job_conf.setMapperClass(bigdatauas.SalesMapper.class);
        job_conf.setReducerClass(bigdatauas.SalesCountryReducer.class);

        // Menentukan format tipe data Input dan output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Mengatur direktori input dan output menggunakan argumen baris perintah,
        // arg[0] = nama direktori input pada HDFS, dan arg[1] = nama direktori output yang akan dibuat untuk menyimpan file output.

        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
        my_client.setConf(job_conf);
        try {
            // Run the job
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}