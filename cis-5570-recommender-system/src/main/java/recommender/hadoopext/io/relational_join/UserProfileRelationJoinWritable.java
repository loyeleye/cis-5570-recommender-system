package recommender.hadoopext.io.relational_join;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserProfileRelationJoinWritable implements Writable {

    private Text relationTable = new Text();
    private IntWritable userId = new IntWritable();
    private IntWritable tagId = new IntWritable();
    private DoubleWritable value = new DoubleWritable();

    public UserProfileRelationJoinWritable() {
    }

    public UserProfileRelationJoinWritable(Text relationTable, DoubleWritable value, IntWritable userId, IntWritable tagId) {
        this.relationTable = relationTable;
        this.userId = userId;
        this.tagId = tagId;
        this.value = value;
    }

    public UserProfileRelationJoinWritable(Text relationTable, DoubleWritable value) {
        this.relationTable = relationTable;
        this.value = value;
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

    public DoubleWritable getValue() {
        return value;
    }

    public void setValue(DoubleWritable value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        relationTable.write(dataOutput);
        value.write(dataOutput);
        tagId.write(dataOutput);
        userId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        relationTable.readFields(dataInput);
        value.readFields(dataInput);
        tagId.readFields(dataInput);
        userId.readFields(dataInput);
    }

    @Override
    public String toString() {
        return (tagId == null) ? String.format("%s:%s", relationTable, value) : String.format("%s:%s:%s", relationTable, value, tagId);
    }
}
