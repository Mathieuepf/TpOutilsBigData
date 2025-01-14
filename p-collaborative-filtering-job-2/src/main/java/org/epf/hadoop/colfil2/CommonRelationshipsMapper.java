package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonRelationshipsMapper extends Mapper<LongWritable, Text, UserPair, Text> {

    private UserPair userPair = new UserPair();
    private Text user = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            return; // Ignore malformed lines
        }

        String userId = line[0];
        String[] relationships = line[1].split(",");

        List<String> relationsList = Arrays.asList(relationships);

        // Generate all pairs of relations
        for (int i = 0; i < relationsList.size(); i++) {
            for (int j = i + 1; j < relationsList.size(); j++) {
                String user1 = relationsList.get(i);
                String user2 = relationsList.get(j);
                userPair.set(user1, user2);
                user.set(userId); // Propagate the originating user
                context.write(userPair, user);
            }
        }
    }
}
