package recommender.hadoopext.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProfileAndTagWritable implements WritableComparable<ProfileAndTagWritable> {
    private ProfileIdWritable profileId;
    private IntWritable tagId;

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
}
