package recommender.hadoopext.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private IntWritable artistIdToJoin = new IntWritable();
    private Text filenameSource = new Text();

    public TaggedKey(IntWritable artistIdJoinKey, Text filename) {
        this.artistIdToJoin = artistIdJoinKey;
        this.filenameSource = filename;
    }

    public TaggedKey() {}

    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.artistIdToJoin.compareTo(taggedKey.getArtistIdToJoin());
        if(compareValue == 0 ){
            compareValue = this.filenameSource.compareTo(taggedKey.getFilenameSource());
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        artistIdToJoin.write(dataOutput);
        filenameSource.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        artistIdToJoin.readFields(dataInput);
        filenameSource.readFields(dataInput);
    }

    public IntWritable getArtistIdToJoin() {
        return artistIdToJoin;
    }

    public void setArtistIdToJoin(IntWritable artistIdToJoin) {
        this.artistIdToJoin = artistIdToJoin;
    }

    public Text getFilenameSource() {
        return filenameSource;
    }

    public void setFilenameSource(Text filenameSource) {
        this.filenameSource = filenameSource;
    }
}