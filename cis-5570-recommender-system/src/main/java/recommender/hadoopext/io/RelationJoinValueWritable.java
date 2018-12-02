package recommender.hadoopext.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelationJoinValueWritable implements Writable {

    private Text relationTable = new Text();
    private IntWritable userId = new IntWritable();
    private IntWritable tagId = new IntWritable();
    private DoubleWritable tagWeight = new DoubleWritable();
    public RelationJoinValueWritable() {
    }
    public RelationJoinValueWritable(Text relationTable, DoubleWritable tagWeight, IntWritable userId, IntWritable tagId) {
        this.relationTable = relationTable;
        this.userId = userId;
        this.tagId = tagId;
        this.tagWeight = tagWeight;
    }

    public RelationJoinValueWritable(Text relationTable, DoubleWritable tagWeight) {
        this.relationTable = relationTable;
        this.tagWeight = tagWeight;
    }

    public IntWritable getUserId() {
        return userId;
    }

    public void setUserId(IntWritable userId) {
        this.userId = userId;
    }

    public IntWritable getTagId() {
        return tagId;
    }

    public void setTagId(IntWritable tagId) {
        this.tagId = tagId;
    }

    public Text getRelationTable() {
        return relationTable;
    }

    public void setRelationTable(Text relationTable) {
        this.relationTable = relationTable;
    }

    public DoubleWritable getTagWeight() {
        return tagWeight;
    }

    public void setTagWeight(DoubleWritable tagWeight) {
        this.tagWeight = tagWeight;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        relationTable.write(dataOutput);
        tagWeight.write(dataOutput);
        tagId.write(dataOutput);
        userId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        relationTable.readFields(dataInput);
        tagWeight.readFields(dataInput);
        tagId.readFields(dataInput);
        userId.readFields(dataInput);
    }

    @Override
    public String toString() {
        return (tagId == null) ? String.format("%s:%s", relationTable, tagWeight) : String.format("%s:%s:%s", relationTable, tagWeight, tagId);
    }
}
