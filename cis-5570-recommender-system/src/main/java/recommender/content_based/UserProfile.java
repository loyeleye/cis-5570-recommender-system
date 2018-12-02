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
    public static class UserProfileJoinMapper
            extends Mapper<Text, RecordWritable, TaggedKey, RelationJoinValueWritable> {
        private final Double WEIGHT_CUTOFF = 0.10;

        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {

            if (Filenames.UA.filename().equalsIgnoreCase(filename.toString())) {
                TaggedKey myTaggedKey = new TaggedKey(record.getArtistId(), filename);
                DoubleWritable weight = new DoubleWritable(record.getWeight().get());
                RelationJoinValueWritable outputValues = new RelationJoinValueWritable(filename, weight, record.getUserId(), new IntWritable(0));
                // Write output to file
                context.write(myTaggedKey, outputValues);
            }
            if ("itemProfile".equalsIgnoreCase(record.getParentFolder().toString())) {
                if (record.getTagWeight().get() > WEIGHT_CUTOFF) {
                    TaggedKey myTaggedKey = new TaggedKey(record.getArtistId(), record.getParentFolder());
                    RelationJoinValueWritable outputValues = new RelationJoinValueWritable(record.getParentFolder(), record.getTagWeight(), new IntWritable(0), record.getTagId());
                    // Write output to file
                    context.write(myTaggedKey, outputValues);
                }
            }
        }
    }

    public static class UserProfileJoinReducer
            extends
            Reducer<TaggedKey, RelationJoinValueWritable, ProfileAndTagWritable, DoubleWritable> {

        private DoubleWritable userTagScore = new DoubleWritable();
        private ProfileAndTagWritable profileAndTag = new ProfileAndTagWritable();

        public void reduce(TaggedKey key, Iterable<RelationJoinValueWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            ArrayList<RelationJoinValueWritable> firstFile = new ArrayList<>();
            String firstFilename = key.getFilenameSource().toString();
            for (RelationJoinValueWritable value : values) {
                if (firstFilename.equals(value.getRelationTable().toString())) {
                    Text f = new Text(value.getRelationTable().toString());
                    IntWritable u = new IntWritable(value.getUserId().get());
                    IntWritable t = new IntWritable(value.getTagId().get());
                    DoubleWritable w = new DoubleWritable(value.getTagWeight().get());
                    RelationJoinValueWritable rjv = new RelationJoinValueWritable(f,w,u,t);
                    firstFile.add(rjv);
                }
                else {
                    if ("itemProfile".equalsIgnoreCase(value.getRelationTable().toString())) {
                        profileAndTag.setTagId(value.getTagId());
                        for (RelationJoinValueWritable userArtist: firstFile) {
                            ProfileIdWritable profileId = new ProfileIdWritable(true, userArtist.getUserId().get());
                            profileAndTag.setProfileId(profileId);
                            userTagScore.set(value.getTagWeight().get() * userArtist.getTagWeight().get());
                            context.write(profileAndTag, userTagScore);
                        }
                    } else {
                        ProfileIdWritable profileId = new ProfileIdWritable(true, value.getUserId().get());
                        profileAndTag.setProfileId(profileId);
                        for (RelationJoinValueWritable itemProfile: firstFile) {
                            profileAndTag.setTagId(itemProfile.getTagId());
                            userTagScore.set(value.getTagWeight().get() * itemProfile.getTagWeight().get());
                            context.write(profileAndTag, userTagScore);
                        }
                    }
                }
            }
        }


    }

    public static class UserProfileAggregateMapper
            extends Mapper<Text, RecordWritable, ProfileAndTagWritable, DoubleWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            ProfileIdWritable profileId = new ProfileIdWritable(true, record.getUserId().get());
            ProfileAndTagWritable profileAndTag = new ProfileAndTagWritable(profileId, record.getTagId());
            context.write(profileAndTag, record.getTagWeight());
        }
    }

    public static class UserProfileAggregateReducer
            extends
            Reducer<ProfileAndTagWritable, DoubleWritable, ProfileAndTagWritable, DoubleWritable> {

        public void reduce(ProfileAndTagWritable profileAndTag, Iterable<DoubleWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            double aggregateTagScore = 0;
            for (DoubleWritable value: values) {
                aggregateTagScore += value.get();
            }
            context.write(profileAndTag, new DoubleWritable(aggregateTagScore));
        }
    }
}
