package recommender.hadoopext.io.inverted_index;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InvertedIndexKeyWritable implements WritableComparable<InvertedIndexKeyWritable> {
    private Boolean isTagFeature;
    private Integer id;

    public InvertedIndexKeyWritable(Boolean isTag, Integer id) {
        this.isTagFeature = isTag;
        this.id = id;
    }

    public InvertedIndexKeyWritable() {}

    public int compareTo(InvertedIndexKeyWritable o) {
        int cmp = isTagFeature.compareTo(o.isTagFeature);
        return (cmp == 0) ? id.compareTo(o.getId()) : cmp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isTagFeature);
        dataOutput.writeInt(id);
    }

    public void readFields(DataInput dataInput) throws IOException {
        isTagFeature = dataInput.readBoolean();
        id = dataInput.readInt();
    }

    public int getId() {
        return this.id;
    }

    public void setId(Integer id) { this.id = id; }

    public Boolean isTag() {
        return isTagFeature;
    }

    @Override
    public String toString() {
        return isTagFeature ? id.toString() : String.valueOf(0);
    }
}
