package recommender.hadoopext.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

public class RecordWritable implements WritableComparable<RecordWritable> {
    private IntWritable userId;
    private IntWritable artistId;
    private IntWritable weight;
    private IntWritable tagId;
    private DoubleWritable tagWeight;

    private Text artistName;
    private Text artistUrl;
    private Text artistPictureUrl;
    private Text tagValue;

    public Text getParentFolder() {
        return parentFolder;
    }

    public void setParentFolder(Text parentFolder) {
        this.parentFolder = parentFolder;
    }

    private Text parentFolder;

    private ArrayWritable misc;

    public static RecordWritable readUserTaggedArtist(String[] record, Text pf) throws IOException {
        RecordWritable r = new RecordWritable();

        // Convert string values
        int u = Integer.parseInt(record[0]);
        int a = Integer.parseInt(record[1]);
        int t = Integer.parseInt(record[2]);

        // Set writables
        r.userId = new IntWritable(u);
        r.artistId = new IntWritable(a);
        r.tagId = new IntWritable(t);
        r.parentFolder = pf;

        return r;
    }

    public static RecordWritable readUserArtist(String[] record, Text pf) {
        RecordWritable r = new RecordWritable();

        // Convert string values
        int u = Integer.parseInt(record[0]);
        int a = Integer.parseInt(record[1]);
        int w = Integer.parseInt(record[2]);

        // Set writables
        r.userId = new IntWritable(u);
        r.artistId = new IntWritable(a);
        r.weight =  new IntWritable(w);
        r.parentFolder = pf;

        return r;
    }

    public static RecordWritable readItemProfile(String[] record, Text pf) {
        RecordWritable r = new RecordWritable();

        // Convert string values
        String artist_profile = record[0];
        String[] artist_tuple = StringUtils.split(artist_profile, ',');
        int artist_id = Integer.parseInt(StringUtils.substringAfter(artist_tuple[0], "-"));
        int tag_id = Integer.parseInt(StringUtils.substringBefore(artist_tuple[1], ")"));
        double score = Double.parseDouble(record[1]);


        // Set writables
        r.artistId = new IntWritable(artist_id);
        r.tagId = new IntWritable(tag_id);
        r.tagWeight =  new DoubleWritable(score);
        r.parentFolder = pf;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        Object o;
        for (Field f: RecordWritable.class.getDeclaredFields()) {
            try {
                 o = f.get(this);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                o = "<error>";
            }
            if (o != null) {
                sb.append(String.format("%s:%s,", f.getName(), o.toString()));
            }
        }
        // remove last comma and add a closing bracket
        sb.setLength(Math.max(sb.length() - 1, 0));
        sb.append("]");
        return sb.toString();
    }

    public DoubleWritable getTagWeight() {
        return tagWeight;
    }
}
