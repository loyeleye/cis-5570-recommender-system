package recommender.hadoopext.io.inverted_index;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InvertedIndexValueWritable implements Writable {
    // Basically the user/item profile
    private String document;
    private Double value;

    public InvertedIndexValueWritable() {}

    public InvertedIndexValueWritable(String document, Double value) {
        this.document = document;
        this.value = value;
    }

    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%s@%s", value.toString(), document);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(document);
        dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        document = dataInput.readUTF();
        value = dataInput.readDouble();
    }
}
