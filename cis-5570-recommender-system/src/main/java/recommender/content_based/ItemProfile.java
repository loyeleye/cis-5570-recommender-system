package recommender.content_based;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.enums.Filenames;
import recommender.hadoopext.io.ProfileAndTagWritable;
import recommender.hadoopext.io.ProfileIdWritable;
import recommender.hadoopext.io.RecordWritable;

import java.io.IOException;

public class ItemProfile {
    /**
     * Step 1 Mapper Class
     * Takes the user_taggedartists file and generates (artist, tag) pairs
     * input: (key: filename, value: record (as RecordWritable))
     * output: (key: artistProfile (as a ProfileIdWritable), value: tagId)
     */
    public static class ItemProfile1Mapper
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
     * Step 1 Reducer Class
     * Gets a total count for all tags associated with a particular artist
     * input: (key: artistProfile (as a ProfileIdWritable), value: tagId)
     * output: (key: (artistProfile,tagId), value: artist-total-tag-count)
     */
    public static class ItemProfile1Reducer
            extends
            Reducer< ProfileIdWritable, IntWritable, ProfileAndTagWritable, IntWritable > {

        private IntWritable artistTotalTagCount = new IntWritable();
        private ProfileAndTagWritable profileAndTag = new ProfileAndTagWritable();

        public void reduce( ProfileIdWritable key, Iterable < IntWritable > values, Context context
        ) throws IOException,
                InterruptedException {
            profileAndTag.setProfileId(key);
            artistTotalTagCount.set(Iterables.size(values));
            for (IntWritable tagId: values) {
                profileAndTag.setTagId(tagId);
                context.write(profileAndTag, artistTotalTagCount);
            }
        }
    }
}
