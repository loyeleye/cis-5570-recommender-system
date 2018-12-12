package recommender.hadoopext.io.recommendation;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import recommender.hadoopext.io.cosine.KeyPair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyPairSecondarySort implements Writable, WritableComparable<KeyPairSecondarySort> {

    private KeyPair keyPair = new KeyPair();
    private Double score;

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public KeyPairSecondarySort(KeyPair kp, Double score) {
        keyPair = kp;
        this.score = score;
    }

    public KeyPairSecondarySort(Integer userId, Integer artistId, Double score) {
        keyPair.setUserId(userId);
        keyPair.setArtistId(artistId);
        this.score = score;
    }

    public Integer getUserId() {
        return keyPair.getUserId();
    }

    public Integer getArtistId() {
        return keyPair.getArtistId();
    }

    private int parseId(String doc) {
        String id = doc.substring(1, doc.length());
        return Integer.parseInt(id);
    }


    public void parseUserId(String doc) {
        keyPair.setUserId(parseId(doc));
    }

    public void parseArtistId(String doc) {
        keyPair.setArtistId(parseId(doc));
    }

    public KeyPairSecondarySort() {}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(keyPair.getUserId());
        dataOutput.writeInt(keyPair.getArtistId());
        dataOutput.writeDouble(score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        keyPair.setUserId(dataInput.readInt());
        keyPair.setArtistId(dataInput.readInt());
        score = dataInput.readDouble();
    }

    @Override
    public int compareTo(KeyPairSecondarySort o) {
        int cmp = this.keyPair.getUserId().compareTo(o.keyPair.getUserId());
        if(cmp==0){
            cmp = this.score.compareTo(o.score) * -1;
        }
        return cmp;
    }


    @Override
    public String toString() {
        return String.format("u%d,a%d > score=%f", keyPair.getUserId(), keyPair.getArtistId(), score);
    }
}