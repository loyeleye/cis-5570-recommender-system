package recommender.hadoopext.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import recommender.deprecated.Feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

public class RecordWritable implements Writable {
    private IntWritable userId;
    private IntWritable artistId;
    private IntWritable weight;
    private IntWritable tagId;
    private DoubleWritable score;
    private IntWritable count;
    private Text featureId;
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
        r.weight = new IntWritable(w);
        r.parentFolder = pf;

        return r;
    }

    public static RecordWritable readUserArtistCount(String[] record, Text pf) {
        RecordWritable r = new RecordWritable();

        // Convert string values
        int userId = Integer.parseInt(record[0]);
        int artist_count = Integer.parseInt(record[1]);

        // Set writables
        r.userId = new IntWritable(userId);
        r.count = new IntWritable(artist_count);
        r.parentFolder = pf;

        return r;
    }

    private static RecordWritable readProfile(String[] record, Text pf, Boolean isUser) {
        RecordWritable r = new RecordWritable();

        // Convert string values
        String profileAndTag = record[0];
        String[] profileTagTuple = StringUtils.split(profileAndTag, ',');
        int profileId = Integer.parseInt(StringUtils.substringAfter(profileTagTuple[0], "-"));
        String feature = StringUtils.substringBefore(profileTagTuple[1], ")");
        double score = Double.parseDouble(record[1]);

        // Set writables
        if ("playcount".equalsIgnoreCase(feature)) {
            r.featureId = new Text(feature);
        } else {
            int tag_id = Integer.parseInt(feature);
            r.tagId = new IntWritable(tag_id);
            r.featureId = new Text("tag");
        }

        if (isUser) {
            r.userId = new IntWritable(profileId);
        } else {
            r.artistId = new IntWritable(profileId);
        }

        r.score = new DoubleWritable(score);
        r.parentFolder = pf;

        return r;
    }

    public static RecordWritable readItemProfile(String[] record, Text pf) {
        return readProfile(record, pf, false);
    }

    public static RecordWritable readUserProfile(String[] record, Text pf) {
        return readProfile(record, pf, true);
    }

    public static RecordWritable readOther(String[] record, Text pf) {
        RecordWritable r = new RecordWritable();

        r.misc = new ArrayWritable(record);
        r.parentFolder = pf;

        return r;
    }

    public Text getFeatureId() {
        return featureId;
    }

    public Text getParentFolder() {
        return parentFolder;
    }

    public void setParentFolder(Text parentFolder) {
        this.parentFolder = parentFolder;
    }

    public void write(DataOutput out) throws IOException {
        userId.write(out);
        artistId.write(out);
        weight.write(out);
        tagId.write(out);

        misc.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        artistId.readFields(in);
        weight.readFields(in);
        tagId.readFields(in);

        misc.readFields(in);
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        Object o;
        for (Field f : RecordWritable.class.getDeclaredFields()) {
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

    public DoubleWritable getScore() {
        return score;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }
}
