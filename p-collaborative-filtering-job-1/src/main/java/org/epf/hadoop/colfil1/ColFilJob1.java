package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ColFilJob1 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "ColFilJob1");

        job.setJarByClass(ColFilJob1.class);
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);
        job.setPartitionerClass(RelationshipPartitioner.class);

        job.setNumReduceTasks(2);
        job.setInputFormatClass(RelationshipInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean jobSuccess = job.waitForCompletion(true);
        System.exit(jobSuccess ? 0 : 1);
    }
}