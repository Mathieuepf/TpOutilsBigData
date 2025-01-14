package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[]args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CommonFriendsJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Common Friends Job");

        job.setJarByClass(ColFilJob2.class);

        // Mapper et Reducer
        job.setMapperClass(CommonRelationshipsMapper.class);
        job.setReducerClass(CommonRelationshipsReducer.class);

        // Clés et valeurs en sortie du Mapper
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(Text.class);

        // Clés et valeurs en sortie du Reducer
        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(Text.class);

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Utiliser 2 reducers
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}