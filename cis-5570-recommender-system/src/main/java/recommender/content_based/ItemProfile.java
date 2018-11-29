package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.enums.Filenames;
import recommender.hadoopext.io.ProfileAndTagWritable;
import recommender.hadoopext.io.ProfileIdWritable;
import recommender.hadoopext.io.RecordWritable;

import java.io.IOException;
import java.util.HashMap;

public class ItemProfile {
    /**
     * Mapper Class
     * Takes the user_taggedartists file and generates (artist, tag) pairs
     * input: (key: filename, value: record (as RecordWritable))
     * output: (key: artistProfile (as a ProfileIdWritable), value: tagId)
     */
    public static class ItemProfileMapper
            extends Mapper<Text, RecordWritable, ProfileIdWritable, IntWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {

            if (Filenames.UT.filename().equalsIgnoreCase(filename.toString())) {
                ProfileIdWritable profileId = new ProfileIdWritable(false, record.getArtistId().get());
                // Write output to file
                context.write(profileId, record.getTagId());
            }

        }
    }

    /**
     * Reducer Class
     * Gets a percentage tag score for all tags associated with a particular artist
     * input: (key: artistProfile (as a ProfileIdWritable), value: tagId)
     * output: (key: (artistProfile,tagId), value: artist-tag-score)
     */
    public static class ItemProfileReducer
            extends
            Reducer< ProfileIdWritable, IntWritable, ProfileAndTagWritable, DoubleWritable > {

        private DoubleWritable artistTagScore = new DoubleWritable();
        private ProfileAndTagWritable profileAndTag = new ProfileAndTagWritable();

        public void reduce( ProfileIdWritable key, Iterable < IntWritable > values, Context context
        ) throws IOException,
                InterruptedException {
            double totalCount = 0;
            HashMap<Integer, Integer> tagCounts = new HashMap<>();
            for (IntWritable tag: values) {
                int tagId = tag.get();
                tagCounts.put(tagId, tagCounts.containsKey(tagId) ? tagCounts.get(tagId) + 1 : 1);
                totalCount+=1;
            }

            profileAndTag.setProfileId(key);

            for (int tagId: tagCounts.keySet()) {
                profileAndTag.setTagId(new IntWritable(tagId));
                artistTagScore.set(tagCounts.get(tagId) / totalCount);
                context.write(profileAndTag, artistTagScore);
            }
        }
    }
}
