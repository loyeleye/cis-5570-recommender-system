package recommender.deprecated;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FeatureValue implements Writable {

    public static final String TAG_ID = "tag";
    public static final String TAG_WEIGHT = "tag-score";
    public static final String TAG_COUNT = "tag-count";
    public static final String PLAYCOUNT = "playcount";


    private Integer intVal;
    private Double doubleVal;
    private boolean isDouble;
    private String name;

    public FeatureValue(String name, Double doubleVal) {
        this.doubleVal = doubleVal;
        this.name = name;
        isDouble = true;
    }

    public FeatureValue(String name, Integer intVal) {
        this.intVal = intVal;
        this.name = name;
        isDouble = false;
    }

    public FeatureValue() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isDouble);
        if (isDouble)
            dataOutput.writeDouble(doubleVal);
        else
            dataOutput.writeInt(intVal);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isDouble = dataInput.readBoolean();
        if (isDouble)
            doubleVal = dataInput.readDouble();
        else
            intVal = dataInput.readInt();
        name = dataInput.readUTF();
    }

    public String getName() {
        return name;
    }

    public void setTagId(Integer tagId) {
        name = TAG_ID;
        isDouble = false;
        intVal = tagId;
        doubleVal = null;
    }

    public void setTagCount(Integer tagCount) {
        name = TAG_COUNT;
        isDouble = false;
        intVal = tagCount;
        doubleVal = null;
    }

    public void setTagScore(Double tagPercentageScore) {
        name = TAG_WEIGHT;
        isDouble = true;
        doubleVal = tagPercentageScore;
        intVal = null;
    }

    public void setPlaycount(Double playcount) {
        name = PLAYCOUNT;
        isDouble = true;
        doubleVal = playcount;
        intVal = null;
    }

    public void setPlaycount(Integer playcount) {
        name = PLAYCOUNT;
        isDouble = false;
        doubleVal = null;
        intVal = playcount;
    }

    public Integer getInt() {
        if (isDouble) {
            System.out.printf("WARNING: %s is a double. Casting return value to integer...", this.toString());
            return doubleVal.intValue();
        }
        return intVal;
    }

    public Double getDouble() {
        if (!isDouble) {
            System.out.printf("WARNING: %s is an integer. Casting return value to double...", this.toString());
            return intVal.doubleValue();
        }
        return doubleVal;
    }

    @Override
    public String toString() {
        return String.format("[%s:%s]", name, isDouble ? doubleVal.toString() : intVal.toString());
    }
}
