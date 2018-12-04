package recommender.hadoopext.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class to represent the identifier/key for a "feature" in a user/item profile
 * The features are intended to be compared between the user and item profile
 * when calculating the similarity score.
 * eg) <userId, featureId> is the feature key for the tag score
 * eg) <userId, 'playcount'> is the feature key for the playcount
 */
public class ProfileFeatureWritable implements WritableComparable<ProfileFeatureWritable> {
    private ProfileIdWritable profileId;
    private Text featureId;

    public ProfileFeatureWritable() {
        this.profileId = new ProfileIdWritable(false, 0);
        this.featureId = new Text("");
    }

    public ProfileFeatureWritable(ProfileIdWritable profileId, String featureId) {
        this.profileId = profileId;
        this.featureId = new Text(featureId);
    }

    public ProfileFeatureWritable(ProfileIdWritable profileId, IntWritable tagId) {
        this.profileId = profileId;
        this.featureId = new Text(tagId.toString());
    }

    public ProfileFeatureWritable(ProfileIdWritable profileId, Text featureId) {
        this.profileId = profileId;
        this.featureId = featureId;
    }

    @Override
    public int compareTo(ProfileFeatureWritable o) {
        int cmp = profileId.compareTo(o.profileId);
        return (cmp == 0) ? featureId.compareTo(o.featureId) : cmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        profileId.write(dataOutput);
        featureId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        profileId.readFields(dataInput);
        featureId.readFields(dataInput);
    }

    public ProfileIdWritable getProfile() {
        return profileId;
    }

    public Text getFeature() {
        return featureId;
    }

    public void setFeature(Text featureId) {
        this.featureId = featureId;
    }

    public void setFeature(String featureId) {
        this.featureId = new Text(featureId);
    }

    public void setTagAsFeature(IntWritable tagId) { this.featureId = new Text(tagId.toString()); }

    public void setProfileId(ProfileIdWritable profileId) {
        this.profileId = profileId;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", profileId.toString(), featureId);
    }
}
