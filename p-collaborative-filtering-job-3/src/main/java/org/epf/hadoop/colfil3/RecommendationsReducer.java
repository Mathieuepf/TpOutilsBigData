package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RecommendationsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Collect recommendations for the user
        List<Recommendation> recommendations = new ArrayList<>();
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length != 2) {
                continue; // Ignore malformed recommendations
            }

            String recommendedUser = parts[0];
            int commonFriendsCount = Integer.parseInt(parts[1]);
            recommendations.add(new Recommendation(recommendedUser, commonFriendsCount));
        }

        // Sort recommendations by common friends count in descending order
        Collections.sort(recommendations, Comparator.comparingInt(Recommendation::getCount).reversed());

        // Select top 5 recommendations
        StringBuilder topRecommendations = new StringBuilder();
        for (int i = 0; i < Math.min(5, recommendations.size()); i++) {
            if (i > 0) {
                topRecommendations.append(",");
            }
            topRecommendations.append(recommendations.get(i).getUser())
                    .append(":")
                    .append(recommendations.get(i).getCount());
        }

        // Emit the user and their top recommendations
        context.write(key, new Text(topRecommendations.toString()));
    }

    // Helper class for recommendations
    private static class Recommendation {
        private final String user;
        private final int count;

        public Recommendation(String user, int count) {
            this.user = user;
            this.count = count;
        }

        public String getUser() {
            return user;
        }

        public int getCount() {
            return count;
        }
    }
}
