package recommender.hadoopext.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelationJoinValueWritable implements Writable {

    public RelationJoinValueWritable() {
    }

    public RelationJoinValueWritable(Text relationTable, DoubleWritable tagWeight) {
        this.relationTable = relationTable;
        this.tagWeight = tagWeight;
    }

    private Text relationTable = new Text();

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

    private DoubleWritable tagWeight = new DoubleWritable();


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        relationTable.write(dataOutput);
        tagWeight.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        relationTable.readFields(dataInput);
        tagWeight.readFields(dataInput);
    }

    @Override
    public String toString() {
        return String.format("%s:%s", relationTable, tagWeight);
    }
}
