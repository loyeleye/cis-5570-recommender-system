package recommender.hadoopext.io.cosine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyPair implements Writable, WritableComparable<KeyPair> {

    private Integer userId = null;
    private Integer artistId = null;

    public void parse(String doc1, String doc2) {
        if (doc1.charAt(0) == 'u') {
            parseUserId(doc1);
            parseArtistId(doc2);
        } else {
            parseUserId(doc2);
            parseArtistId(doc1);
        }
    }

    public Integer getUserId() {
        return userId;
    }

    public Integer getArtistId() {
        return artistId;
    }

    private int parseId(String doc) {
        String id = doc.substring(1, doc.length());
        return Integer.parseInt(id);
    }


    public void parseUserId(String doc) {
        userId = parseId(doc);
    }

    public void parseArtistId(String doc) {
        artistId = parseId(doc);
    }

    public KeyPair() {}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userId);
        dataOutput.writeInt(artistId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readInt();
        artistId = dataInput.readInt();
    }

    @Override
    public int compareTo(KeyPair o) {
        int cmp = this.userId.compareTo(o.userId);
        if(cmp == 0 ){
            cmp = this.artistId.compareTo(o.artistId);
        }
        return cmp;
    }


    @Override
    public String toString() {
        return String.format("u%d,a%d", userId, artistId);
    }
}