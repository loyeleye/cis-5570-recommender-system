package recommender.hadoopext.io.relational_join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelationalJoinKey implements Writable, WritableComparable<RelationalJoinKey> {

    private IntWritable commonIdToJoin = new IntWritable();
    private Text relationalTable = new Text();

    public RelationalJoinKey(IntWritable commonIdToJoin, Text relationalTable) {
        this.commonIdToJoin = commonIdToJoin;
        this.relationalTable = relationalTable;
    }

    public RelationalJoinKey() {}

    @Override
    public int compareTo(RelationalJoinKey relationalJoinKey) {
        int compareValue = this.commonIdToJoin.compareTo(relationalJoinKey.getCommonIdToJoin());
        if(compareValue == 0 ){
            compareValue = this.relationalTable.compareTo(relationalJoinKey.getRelationalTable());
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        commonIdToJoin.write(dataOutput);
        relationalTable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commonIdToJoin.readFields(dataInput);
        relationalTable.readFields(dataInput);
    }

    public IntWritable getCommonIdToJoin() {
        return commonIdToJoin;
    }

    public void setCommonIdToJoin(IntWritable commonIdToJoin) {
        this.commonIdToJoin = commonIdToJoin;
    }

    public Text getRelationalTable() {
        return relationalTable;
    }

    public void setRelationalTable(Text relationalTable) {
        this.relationalTable = relationalTable;
    }
}