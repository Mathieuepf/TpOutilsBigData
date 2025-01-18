package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonRelationshipsReducer extends Reducer<UserPair, Text, UserPair, Text> {

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (Text ignored : values) {
            count++;
        }
        
        context.write(key, new Text(String.valueOf(count)));
    }
}
