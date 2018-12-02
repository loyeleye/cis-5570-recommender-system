package recommender.hadoopext.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProfileAndTagWritable implements WritableComparable<ProfileAndTagWritable> {
    private ProfileIdWritable profileId;
    private IntWritable tagId;

    public ProfileAndTagWritable() {
        this.profileId = new ProfileIdWritable(false, 0);
        this.tagId = new IntWritable(0);
    }

    public ProfileAndTagWritable(ProfileIdWritable profileId, IntWritable tagId) {
        this.profileId = profileId;
        this.tagId = tagId;
    }

    @Override
    public int compareTo(ProfileAndTagWritable o) {
        int cmp = profileId.compareTo(o.profileId);
        return (cmp == 0) ? tagId.compareTo(o.tagId) : cmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        profileId.write(dataOutput);
        tagId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        profileId.readFields(dataInput);
        tagId.readFields(dataInput);
    }

    public ProfileIdWritable getProfile() {
        return profileId;
    }

    public IntWritable getTag() {
        return tagId;
    }

    public void setTagId(IntWritable tagId) {
        this.tagId = tagId;
    }

    public void setProfileId(ProfileIdWritable profileId) {
        this.profileId = profileId;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", profileId.toString(), tagId);
    }
}
