package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelationshipPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        if (key == null || key.toString().isEmpty()) {
            return 0;
        }
        char firstChar = key.toString().charAt(0);
        int partition = (Character.toLowerCase(firstChar) - 'a') % numPartitions;
        if (partition < 0) {
            partition = 0;
        }
        return partition;
    }
}