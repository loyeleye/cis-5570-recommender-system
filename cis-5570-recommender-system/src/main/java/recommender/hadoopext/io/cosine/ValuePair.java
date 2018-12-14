package recommender.hadoopext.io.cosine;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class
ValuePair implements Writable {
    private Double v1;
    private Double v2;

    public ValuePair() {
    }

    public ValuePair(Double v1, Double v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public Double getV1() {
        return v1;
    }

    public void setV1(Double v1) {
        this.v1 = v1;
    }

    public Double getV2() {
        return v2;
    }

    public void setV2(Double v2) {
        this.v2 = v2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(v1);
        dataOutput.writeDouble(v2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        v1 = dataInput.readDouble();
        v2 = dataInput.readDouble();
    }
}
