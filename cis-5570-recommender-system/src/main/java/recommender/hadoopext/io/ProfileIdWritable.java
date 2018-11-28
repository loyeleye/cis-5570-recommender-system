package recommender.hadoopext.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProfileIdWritable implements WritableComparable<ProfileIdWritable> {
    // Two types of profiles, user and item. This boolean tells us profile type.
    private Boolean isUserProfile;
    private Integer id;

    public ProfileIdWritable(Boolean isUserProfile, Integer id) {
        this.isUserProfile = isUserProfile;
        this.id = id;
    }

    public ProfileIdWritable() {}

    public int compareTo(ProfileIdWritable o) {
        int cmp = isUserProfile.compareTo(o.isUserProfile);
        return (cmp == 0) ? id.compareTo(o.getId()) : cmp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isUserProfile);
        dataOutput.writeInt(id);
    }

    public void readFields(DataInput dataInput) throws IOException {
        isUserProfile = dataInput.readBoolean();
        id = dataInput.readInt();
    }

    public int getId() {
        return this.id;
    }

    public Boolean isUser() {
        return isUserProfile;
    }

    public Boolean isArtist() {
        return !isUserProfile;
    }

    @Override
    public String toString() {
        return String.format("%s-%d", isUserProfile ? "user" : "artist", id);
    }
}
