package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationsMapper extends Mapper<Object, Text, Text, Text> {

    private Text userKey = new Text();
    private Text recommendationValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            return;
        }

        String[] users = line[0].split(",");
        if (users.length != 2) {
            return;
        }

        String userA = users[0];
        String userB = users[1];
        String commonFriendsCount = line[1];

        userKey.set(userA);
        recommendationValue.set(userB + "," + commonFriendsCount);
        context.write(userKey, recommendationValue);

        userKey.set(userB);
        recommendationValue.set(userA + "," + commonFriendsCount);
        context.write(userKey, recommendationValue);
    }
}
