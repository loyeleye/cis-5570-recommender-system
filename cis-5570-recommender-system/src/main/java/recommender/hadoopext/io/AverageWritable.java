package recommender.hadoopext.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Average Writable:
 */
public class AverageWritable implements Writable {
    private double sum;
    private int count;

    public AverageWritable(Double sum, Integer count) {
        this.sum = sum;
        this.count = count;
    }

    public AverageWritable() {}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sum);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum = dataInput.readDouble();
        count = dataInput.readInt();
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void add(AverageWritable o) {
        this.sum += o.sum;
        this.count += o.count;
    }

    public double getAverage() {
        return (sum / count);
    }

    @Override
    public String toString() {
        return String.format("Avg[Sum=%f,Cnt=%d]=%f", sum, count, getAverage());
    }
}
