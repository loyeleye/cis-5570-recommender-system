package recommender.data_type;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

    @Override
    public int compareTo(ProfileIdWritable o) {
        // TODO: distinguish between user and item profile
        return id.compareTo(o.getId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isUserProfile);
        dataOutput.writeInt(id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isUserProfile = dataInput.readBoolean();
        id = dataInput.readInt();
    }

    public int getId() {
        return this.id;
    }
}
