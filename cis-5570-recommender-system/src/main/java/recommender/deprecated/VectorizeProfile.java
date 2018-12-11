package recommender.deprecated;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.deprecated.NamedDoubleWritable;
import recommender.deprecated.ProfileVectorWritable;
import recommender.enums.FileFolders;
import recommender.hadoopext.io.ProfileFeatureWritable;
import recommender.hadoopext.io.ProfileIdWritable;
import recommender.hadoopext.io.RecordWritable;

import java.io.IOException;
import java.util.ArrayList;

class VectorizeProfile {
    public static class VectorizeProfileMapper
            extends
            Mapper<Text, RecordWritable, ProfileFeatureWritable, NamedDoubleWritable> {
        static final String USER_PROFILE_TAGS = FileFolders.UP_TW.foldername();
        static final String ITEM_PROFILE_TAGS = FileFolders.IP_TW.foldername();
        static final String USER_PROFILE_PLAYS = FileFolders.UP_PC.foldername();
        static final String ITEM_PROFILE_PLAYS = FileFolders.IP_PC.foldername();

        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            try {
                String folderName = record.getParentFolder().toString();
                if (USER_PROFILE_TAGS.equalsIgnoreCase(folderName)) {
                    ProfileIdWritable profileId = new ProfileIdWritable(true, record.getUserId().get());
                    ProfileFeatureWritable featureKey = new ProfileFeatureWritable(profileId, record.getTagId());
                    NamedDoubleWritable featureValue = new NamedDoubleWritable(record.getTagId().toString(), record.getScore().get());
                    context.write(featureKey, featureValue);
                }
                if (USER_PROFILE_PLAYS.equalsIgnoreCase(folderName)) {
                    ProfileIdWritable profileId = new ProfileIdWritable(true, record.getUserId().get());
                    ProfileFeatureWritable featureKey = new ProfileFeatureWritable(profileId, record.getFeatureId());
                    NamedDoubleWritable featureValue = new NamedDoubleWritable(record.getFeatureId().toString(), record.getScore().get());
                    context.write(featureKey, featureValue);
                }
                if (ITEM_PROFILE_TAGS.equalsIgnoreCase(folderName)) {
                    ProfileIdWritable profileId = new ProfileIdWritable(false, record.getArtistId().get());
                    ProfileFeatureWritable featureKey = new ProfileFeatureWritable(profileId, record.getTagId());
                    NamedDoubleWritable featureValue = new NamedDoubleWritable(record.getTagId().toString(), record.getScore().get());
                    context.write(featureKey, featureValue);
                }
                if (ITEM_PROFILE_PLAYS.equalsIgnoreCase(folderName)) {
                    ProfileIdWritable profileId = new ProfileIdWritable(false, record.getArtistId().get());
                    ProfileFeatureWritable featureKey = new ProfileFeatureWritable(profileId, record.getFeatureId());
                    NamedDoubleWritable featureValue = new NamedDoubleWritable(record.getFeatureId().toString(), record.getScore().get());
                    context.write(featureKey, featureValue);
                }
            } catch (Exception e) {
                System.out.println(String.format("File failed on:\nfile %s\nparent folder %s\n record %s\nException: %s",
                        filename.toString(), record.getParentFolder(), record.toString(), e.getMessage()));
                throw new IOException(e);
            }

        }
    }

    public static class VectorizeProfileReducer
            extends
            Reducer<ProfileFeatureWritable, NamedDoubleWritable, ProfileIdWritable, ProfileVectorWritable> {
        public void reduce(ProfileFeatureWritable key, Iterable<NamedDoubleWritable> features, Context context) throws IOException, InterruptedException {
            ArrayList<NamedDoubleWritable> featureVector = new ArrayList<>();
            for (NamedDoubleWritable feature : features) {
                NamedDoubleWritable f = new NamedDoubleWritable(feature.getName(), feature.getValue());
                featureVector.add(f);
            }
            ProfileVectorWritable writableVector = new ProfileVectorWritable(featureVector.toArray(new NamedDoubleWritable[]{}));
            // ProfileFeatureWritable profileAndVectorSize = new ProfileFeatureWritable(key.getProfile(), String.valueOf(writableVector.get().length));
            // If the only value in the vector is playcount, don't bother writing out a profile vector
            if (writableVector.get().length > 1) {
                context.write(key.getProfile(), writableVector);
            }
        }
    }

    public static class CosineMapper
            extends
            Mapper<Text, RecordWritable, Text, Text> {
        public void map(Text filename, RecordWritable record, Context context) {

        }
    }

    public static class CosineReducer
            extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Text value, Context context) {

        }
    }
}
