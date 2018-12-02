package recommender.hadoopext.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntegerPair implements Writable, WritableComparable<IntegerPair> {
    private Integer v1;
    private Integer v2;

    @Override
    public int compareTo(IntegerPair o) {
        int cmp = v1.compareTo(o.v1);
        return (cmp == 0) ? v2.compareTo(o.v2) : cmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(v1);
        dataOutput.writeInt(v2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        v1 = dataInput.readInt();
        v2 = dataInput.readInt();
    }

    public IntegerPair(Integer v1, Integer v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public Integer getV1() {
        return v1;
    }

    public void setV1(Integer v1) {
        this.v1 = v1;
    }

    public Integer getV2() {
        return v2;
    }

    public void setV2(Integer v2) {
        this.v2 = v2;
    }

    public Integer getLeft() {
        return getV1();
    }

    public void setLeft(Integer left) {
        setV1(left);
    }

    public Integer getRight() {
        return getV2();
    }

    public void setRight(Integer right) {
        setV2(right);
    }
}
