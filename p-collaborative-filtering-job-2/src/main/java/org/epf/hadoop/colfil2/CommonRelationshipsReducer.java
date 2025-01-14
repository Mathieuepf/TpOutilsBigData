package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonRelationshipsReducer extends Reducer<UserPair, Text, UserPair, Text> {

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        // Count the number of values for each pair
        for (Text ignored : values) {
            count++;
        }

        // Output the pair and the count of common relationships
        context.write(key, new Text(String.valueOf(count)));
    }
}
