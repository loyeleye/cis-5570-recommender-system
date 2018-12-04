package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.enums.Filenames;
import recommender.hadoopext.io.*;


import java.io.IOException;
import java.util.ArrayList;

class UserProfile {

    /**
     * We do a relational join when attempting to find the tag weights for the user profile to make sure that for each user,
     * there is a tag weight *feature* corresponding to the artists they have listened to. We inner join the item_profile
     * "table" we have just generated with the user_artists table using the artist id.
     * ----------------------------------------------
     * Item profile schema ==> [artist] [tag] [tag-weight]
     * User artist schema ==> [user] [artist] [weight]
     * ----------------------------------------------
     * Final User profile schema (reducer output) ==> [user] [tag] [tag-weight] one for each artist
     */
    public static class UserProfileRelationJoinMapper
            extends Mapper<Text, RecordWritable, JoinByArtistKey, UserProfileRelationJoinWritable> {
        // Any tag weight that is lower than this threshold will be filtered and not used to calculate the final recommendation.
        // This is necessary only because the mapper will generate an large number of intermediate values, many of which
        // really should not have much impact on the final recommendation (similarity) score.
        private final Double ITEM_PROFILE_TAG_WEIGHT_CUTOFF = 0.10;

        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            // If the filename the record being read from is "user_artists.dat"
            if (Filenames.UA.filename().equalsIgnoreCase(filename.toString())) {
                JoinByArtistKey myJoinByArtistKey = new JoinByArtistKey(record.getArtistId(), filename);
                DoubleWritable weight = new DoubleWritable(record.getWeight().get());
                UserProfileRelationJoinWritable outputValues = new UserProfileRelationJoinWritable(filename, weight, record.getUserId(), new IntWritable(0));
                // Write output to file
                context.write(myJoinByArtistKey, outputValues);
            }
            // If the filename the record being read from is from the folder "itemProfile"
            if ("itemProfile".equalsIgnoreCase(record.getParentFolder().toString())) {
                if (record.getScore().get() > ITEM_PROFILE_TAG_WEIGHT_CUTOFF) {
                    JoinByArtistKey joinByArtistKey = new JoinByArtistKey(record.getArtistId(), record.getParentFolder());
                    UserProfileRelationJoinWritable outputValues = new UserProfileRelationJoinWritable(record.getParentFolder(), record.getScore(), new IntWritable(0), record.getTagId());
                    // Write output to file
                    context.write(joinByArtistKey, outputValues);
                }
            }
        }
    }

    /**
     * We are doing a *reduce side join* here using the [artist-id] as the primary key and [relational_table]
     * (aka the file the record comes from) as the secondary key for sorting.
     * We've created a custom partitioner and grouping class so that the secondary key is ignored when interpreting
     * which values to send to which reducers. It is only used for sorting.
     * ----------------------------------------------
     * Final Output: User profile schema ==> [user] [tag] [tag-weight] one for each artist
     */
    public static class UserProfileRelationJoinReducer
            extends
            Reducer<JoinByArtistKey, UserProfileRelationJoinWritable, ProfileFeatureWritable, DoubleWritable> {
        public void reduce(JoinByArtistKey key, Iterable<UserProfileRelationJoinWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            ArrayList<UserProfileRelationJoinWritable> firstFile = new ArrayList<>();
            String firstFilename = key.getFilenameSource().toString();
            DoubleWritable userTagScore = new DoubleWritable();
            ProfileFeatureWritable profileAndTag = new ProfileFeatureWritable();
            ProfileIdWritable profileId = new ProfileIdWritable(true, null);
            profileAndTag.setProfileId(profileId);

            for (UserProfileRelationJoinWritable value : values) {
                if (firstFilename.equals(value.getRelationTable().toString())) {
                    Text relationalTable = new Text(value.getRelationTable().toString());
                    IntWritable userId = new IntWritable(value.getUserId().get());
                    IntWritable tagId = new IntWritable(value.getTagId().get());
                    DoubleWritable tagWeight = new DoubleWritable(value.getTagWeight().get());
                    UserProfileRelationJoinWritable rjv = new UserProfileRelationJoinWritable(relationalTable,tagWeight,userId,tagId);
                    firstFile.add(rjv);
                }
                else {
                    if ("itemProfile".equalsIgnoreCase(value.getRelationTable().toString())) {
                        userTagScore.set(value.getTagWeight().get());
                        profileAndTag.setTagAsFeature(value.getTagId());
                        for (UserProfileRelationJoinWritable userArtist: firstFile) {
                            profileId.setId(userArtist.getUserId().get());
                            context.write(profileAndTag, userTagScore);
                        }
                    } else {
                        profileId.setId(value.getUserId().get());
                        for (UserProfileRelationJoinWritable itemProfile: firstFile) {
                            userTagScore.set(itemProfile.getTagWeight().get());
                            profileAndTag.setTagAsFeature(itemProfile.getTagId());
                            context.write(profileAndTag, userTagScore);
                        }
                    }
                }
            }
        }
    }

    /**
     * Given a list of user features: one for each individual <user,artist,tag> combination:
     * Get the average for each <user,tag> pair.
     */
    public static class UserProfileTagWeightAveragingMapper
            extends Mapper<Text, RecordWritable, ProfileFeatureWritable, AverageWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            ProfileIdWritable profileId = new ProfileIdWritable(true, record.getUserId().get());
            ProfileFeatureWritable profileAndTag = new ProfileFeatureWritable(profileId, record.getTagId());
            AverageWritable tagWeight = new AverageWritable(record.getScore().get(), 1);
            // Write output to file
            context.write(profileAndTag, tagWeight);
        }
    }

    /**
     * Given a list of user features: one for each individual <user,artist,tag> combination:
     * Get the average for each <user,tag> pair.
     */
    public static class UserProfileTagWeightAveragingReducer
            extends
            Reducer<ProfileFeatureWritable, AverageWritable, ProfileFeatureWritable, DoubleWritable> {

        public void reduce(ProfileFeatureWritable profileAndTag, Iterable<AverageWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            AverageWritable average = new AverageWritable();
            for (AverageWritable value: values) {
                average.add(value);
            }
            context.write(profileAndTag, new DoubleWritable(average.getAverage()));
        }
    }

    public static class UserProfileWeightMapper
            extends Mapper<Text, RecordWritable, ProfileIdWritable, AverageWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            if (Filenames.UA.filename().equalsIgnoreCase(filename.toString())) {
                ProfileIdWritable profileId = new ProfileIdWritable(true, record.getUserId().get());
                AverageWritable weight = new AverageWritable((double) record.getWeight().get(), 1);
                // Write output to file
                context.write(profileId, weight);
            }
        }
    }

    public static class UserProfileWeightReducer
            extends
            Reducer<ProfileIdWritable, AverageWritable, ProfileIdWritable, DoubleWritable> {

        public void reduce(ProfileIdWritable profileId, Iterable<AverageWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            AverageWritable average = new AverageWritable();
            for (AverageWritable value: values) {
                average.add(value);
            }
            context.write(profileId, new DoubleWritable(average.getAverage()));
        }
    }
}
