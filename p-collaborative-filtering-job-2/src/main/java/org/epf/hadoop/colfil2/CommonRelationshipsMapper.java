package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CommonRelationshipsMapper extends Mapper<Object, Text, UserPair, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        if (line.length != 2) return;

        String user = line[0];
        String[] friends = line[1].split(",");

        // Utiliser un set pour éviter les doublons
        Set<String> friendSet = new HashSet<>(Arrays.asList(friends));

        for (String friend1 : friendSet) {
            for (String friend2 : friendSet) {
                if (!friend1.equals(friend2)) {
                    UserPair pair = new UserPair(friend1, friend2);
                    context.write(pair, new Text(user));
                }
            }
        }
    }
}
