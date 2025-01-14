package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CommonRelationshipsReducer extends Reducer<UserPair, Text, UserPair, Text> {

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> commonUsers = new HashSet<>();
        for (Text value : values) {
            commonUsers.add(value.toString());
        }

        // Écrire le résultat final
        context.write(key, new Text(String.valueOf(commonUsers.size())));
    }
}
