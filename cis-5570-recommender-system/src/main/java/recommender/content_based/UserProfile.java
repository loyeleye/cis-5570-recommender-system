package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.enums.FileFolders;
import recommender.enums.Filenames;
import recommender.hadoopext.io.*;
import recommender.hadoopext.io.relational_join.RelationalJoinKey;
import recommender.hadoopext.io.relational_join.UserProfileRelationJoinWritable;


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
            extends Mapper<Text, RecordWritable, RelationalJoinKey, UserProfileRelationJoinWritable> {
        // Any tag weight that is lower than this threshold will be filtered and not used to calculate the final recommendation.
        // This is necessary only because the mapper will generate an large number of intermediate values, many of which
        // really should not have much impact on the final recommendation (similarity) score.
        private final Double ITEM_PROFILE_TAG_WEIGHT_CUTOFF = 0.10;

        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            // If the filename the record being read from is "user_artists.dat"
            if (Filenames.UA.filename().equalsIgnoreCase(filename.toString())) {
                RelationalJoinKey myRelationalJoinKey = new RelationalJoinKey(record.getArtistId(), filename);
                DoubleWritable weight = new DoubleWritable(record.getWeight().get());
                UserProfileRelationJoinWritable outputValues = new UserProfileRelationJoinWritable(filename, weight, record.getUserId(), new IntWritable(0));
                // Write output to file
                context.write(myRelationalJoinKey, outputValues);
            }
            // If the filename the record being read from is from the folder "itemProfile"
            if ("itemProfile".equalsIgnoreCase(record.getParentFolder().toString())) {
                if (record.getScore().get() > ITEM_PROFILE_TAG_WEIGHT_CUTOFF) {
                    RelationalJoinKey relationalJoinKey = new RelationalJoinKey(record.getArtistId(), record.getParentFolder());
                    UserProfileRelationJoinWritable outputValues = new UserProfileRelationJoinWritable(record.getParentFolder(), record.getScore(), new IntWritable(0), record.getTagId());
                    // Write output to file
                    context.write(relationalJoinKey, outputValues);
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
            Reducer<RelationalJoinKey, UserProfileRelationJoinWritable, ProfileFeatureWritable, DoubleWritable> {
        public void reduce(RelationalJoinKey key, Iterable<UserProfileRelationJoinWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            ArrayList<UserProfileRelationJoinWritable> firstFile = new ArrayList<>();
            String firstFilename = key.getRelationalTable().toString();
            DoubleWritable userTagScore = new DoubleWritable();
            ProfileFeatureWritable profileAndTag = new ProfileFeatureWritable();
            ProfileIdWritable profileId = new ProfileIdWritable(true, null);
            profileAndTag.setProfileId(profileId);

            for (UserProfileRelationJoinWritable value : values) {
                if (firstFilename.equals(value.getRelationTable().toString())) {
                    Text relationalTable = new Text(value.getRelationTable().toString());
                    IntWritable userId = new IntWritable(value.getUserId().get());
                    IntWritable tagId = new IntWritable(value.getTagId().get());
                    DoubleWritable tagWeight = new DoubleWritable(value.getValue().get());
                    UserProfileRelationJoinWritable rjv = new UserProfileRelationJoinWritable(relationalTable, tagWeight, userId, tagId);
                    firstFile.add(rjv);
                } else {
                    if ("itemProfile".equalsIgnoreCase(value.getRelationTable().toString())) {
                        userTagScore.set(value.getValue().get());
                        profileAndTag.setTagAsFeature(value.getTagId());
                        for (UserProfileRelationJoinWritable userArtist : firstFile) {
                            profileId.setId(userArtist.getUserId().get());
                            context.write(profileAndTag, userTagScore);
                        }
                    } else {
                        profileId.setId(value.getUserId().get());
                        for (UserProfileRelationJoinWritable itemProfile : firstFile) {
                            userTagScore.set(itemProfile.getValue().get());
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
    public static class UserProfileTagWeightAggregatorMapper
            extends Mapper<Text, RecordWritable, ProfileFeatureWritable, DoubleWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            ProfileIdWritable profileId = new ProfileIdWritable(true, record.getUserId().get());
            ProfileFeatureWritable profileAndTag = new ProfileFeatureWritable(profileId, record.getTagId());
            // Write output to file
            context.write(profileAndTag, new DoubleWritable(record.getScore().get()));
        }
    }

    /**
     * Given a list of user features: one for each individual <user,artist,tag> combination:
     * Get the average for each <user,tag> pair.
     */
    public static class UserProfileTagWeightAggregatorReducer
            extends
            Reducer<ProfileFeatureWritable, DoubleWritable, ProfileFeatureWritable, DoubleWritable> {

        public void reduce(ProfileFeatureWritable profileAndTag, Iterable<DoubleWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(profileAndTag, new DoubleWritable(sum));
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
            Reducer<ProfileIdWritable, AverageWritable, ProfileFeatureWritable, DoubleWritable> {

        public void reduce(ProfileIdWritable profileId, Iterable<AverageWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            ProfileFeatureWritable profileAndWeight = new ProfileFeatureWritable();
            profileAndWeight.setProfileId(profileId);

            AverageWritable average = new AverageWritable();
            for (AverageWritable value : values) {
                average.add(value);
            }

            profileAndWeight.setFeature("playcount");
            context.write(profileAndWeight, new DoubleWritable(average.getAverage() * Main.PLAYCOUNT_ALPHA));
        }
    }

    public static class UserProfileTagWeightNormalizedMapper
            extends Mapper<Text, RecordWritable, RelationalJoinKey, UserProfileRelationJoinWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            try {
                // If the filename the record being read from is from the folder  "userProfNonNorm"
                if (FileFolders.UP_NN.foldername().equalsIgnoreCase(record.getParentFolder().toString())) {
                    RelationalJoinKey myRelationalJoinKey = new RelationalJoinKey(record.getUserId(), record.getParentFolder());
                    DoubleWritable tagWeightNonNormalized = new DoubleWritable(record.getScore().get());
                    UserProfileRelationJoinWritable outputValues = new UserProfileRelationJoinWritable(record.getParentFolder(), tagWeightNonNormalized, record.getUserId(), record.getTagId());
                    // Write output to file
                    context.write(myRelationalJoinKey, outputValues);
                }
                // If the filename the record being read from is from the folder "userArtistCount"
                if (FileFolders.UA_CT.foldername().equalsIgnoreCase(record.getParentFolder().toString())) {
                    RelationalJoinKey relationalJoinKey = new RelationalJoinKey(record.getUserId(), record.getParentFolder());
                    DoubleWritable count = new DoubleWritable(record.getCount().get());
                    UserProfileRelationJoinWritable outputValues = new UserProfileRelationJoinWritable(record.getParentFolder(), count, record.getUserId(), new IntWritable(0));
                    // Write output to file
                    context.write(relationalJoinKey, outputValues);
                }
            } catch (Exception e) {
                System.out.println(String.format("File failed on:\nfile %s\nparent folder %s\n record %s\nException: %s",
                        filename.toString(), record.getParentFolder(), record.toString(), e.getMessage()));
                throw new IOException(e);
            }
        }
    }

    public static class UserProfileTagWeightNormalizedReducer
            extends Reducer<RelationalJoinKey, UserProfileRelationJoinWritable, ProfileFeatureWritable, DoubleWritable> {
        public void reduce(RelationalJoinKey relationalJoinKey, Iterable<UserProfileRelationJoinWritable> tableElements, Context context
        ) throws IOException, InterruptedException {
            ArrayList<UserProfileRelationJoinWritable> userArtistCount = new ArrayList<>();
            ArrayList<UserProfileRelationJoinWritable> userProfile = new ArrayList<>();
            DoubleWritable normalizedTagWeight = new DoubleWritable();
            ProfileFeatureWritable profileAndTag = new ProfileFeatureWritable();
            ProfileIdWritable profileId = new ProfileIdWritable(true, relationalJoinKey.getCommonIdToJoin().get());
            profileAndTag.setProfileId(profileId);

            for (UserProfileRelationJoinWritable value : tableElements) {
                if (FileFolders.UP_NN.foldername().equals(value.getRelationTable().toString())) {
                    Text relationalTable = new Text(value.getRelationTable().toString());
                    IntWritable userId = new IntWritable(value.getUserId().get());
                    IntWritable tagId = new IntWritable(value.getTagId().get());
                    DoubleWritable tagWeight = new DoubleWritable(value.getValue().get());
                    UserProfileRelationJoinWritable rjv = new UserProfileRelationJoinWritable(relationalTable, tagWeight, userId, tagId);
                    userProfile.add(rjv);
                } else if (FileFolders.UA_CT.foldername().equals(value.getRelationTable().toString())) {
                    Text relationalTable = new Text(value.getRelationTable().toString());
                    IntWritable userId = new IntWritable(value.getUserId().get());
                    IntWritable tagId = new IntWritable(value.getTagId().get());
                    DoubleWritable tagWeight = new DoubleWritable(value.getValue().get());
                    UserProfileRelationJoinWritable rjv = new UserProfileRelationJoinWritable(relationalTable, tagWeight, userId, tagId);
                    userArtistCount.add(rjv);
                }
            }

            for (UserProfileRelationJoinWritable up : userProfile) {
                double tagWeight = up.getValue().get();
                double artistCount = userArtistCount.get(0).getValue().get();
                up.getValue().set(tagWeight / artistCount);
            }

            double norm = 0;
            for (UserProfileRelationJoinWritable up : userProfile) {
                norm += Math.pow(up.getValue().get(), 2);
            }
            norm = Math.sqrt(norm);

            for (UserProfileRelationJoinWritable up : userProfile) {
                profileAndTag.setTagAsFeature(up.getTagId());
                normalizedTagWeight.set(up.getValue().get() / norm);
                context.write(profileAndTag, normalizedTagWeight);
            }
        }
    }
}