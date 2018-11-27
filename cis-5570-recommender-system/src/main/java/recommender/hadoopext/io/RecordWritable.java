package recommender.hadoopext.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordWritable implements WritableComparable<RecordWritable> {
    private IntWritable userId;
    private IntWritable artistId;
    private IntWritable weight;
    private IntWritable tagId;

    private Text artistName;
    private Text artistUrl;
    private Text artistPictureUrl;
    private Text tagValue;

    private ArrayWritable misc;

    public static RecordWritable readUserTaggedArtist(String[] record) {
        RecordWritable r = new RecordWritable();

        // Convert string values
        int u = Integer.parseInt(record[0]);
        int a = Integer.parseInt(record[1]);
        int t = Integer.parseInt(record[2]);

        // Set writables
        r.userId.set(u);
        r.artistId.set(a);
        r.tagId.set(t);

        return r;
    }

    public static RecordWritable readUserArtist(String[] record) {
        RecordWritable r = new RecordWritable();

        // Convert string values
        int u = Integer.parseInt(record[0]);
        int a = Integer.parseInt(record[1]);
        int w = Integer.parseInt(record[2]);

        // Set writables
        r.userId.set(u);
        r.artistId.set(a);
        r.weight.set(w);

        return r;
    }

    public static RecordWritable readOther(String[] record) {
        RecordWritable r = new RecordWritable();

        r.misc = new ArrayWritable(record);

        return r;
    }

    public void write(DataOutput out) throws IOException {
        userId.write(out);
        artistId.write(out);
        weight.write(out);
        tagId.write(out);

        artistName.write(out);
        artistUrl.write(out);
        artistPictureUrl.write(out);
        tagValue.write(out);

        misc.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        artistId.readFields(in);
        weight.readFields(in);
        tagId.readFields(in);

        artistName.readFields(in);
        artistUrl.readFields(in);
        artistPictureUrl.readFields(in);
        tagValue.readFields(in);

        misc.readFields(in);
    }

    public int compareTo(RecordWritable o) {
        int cmp = userId.compareTo(o.userId);

        if (cmp == 0) cmp = artistId.compareTo(o.artistId);
        if (cmp == 0) cmp = tagId.compareTo(o.tagId);
        if (cmp == 0) cmp = weight.compareTo(o.weight);

        return cmp;
    }

    public IntWritable getUserId() {
        return userId;
    }

    public IntWritable getArtistId() {
        return artistId;
    }

    public IntWritable getWeight() {
        return weight;
    }

    public IntWritable getTagId() {
        return tagId;
    }

    public Text getArtistName() {
        return artistName;
    }

    public Text getArtistUrl() {
        return artistUrl;
    }

    public Text getArtistPictureUrl() {
        return artistPictureUrl;
    }

    public Text getTagValue() {
        return tagValue;
    }

    public ArrayWritable getMisc() {
        return misc;
    }
}
