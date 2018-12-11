package recommender.deprecated;

import org.apache.hadoop.io.ArrayWritable;
import recommender.deprecated.NamedDoubleWritable;

import java.util.Arrays;

public class ProfileVectorWritable extends ArrayWritable {
    public ProfileVectorWritable(NamedDoubleWritable[] values) {
        super(NamedDoubleWritable.class, values);
    }

    @Override
    public NamedDoubleWritable[] get() {
        return (NamedDoubleWritable[]) super.get();
    }

    @Override
    public String toString() {
        return Arrays.toString(get()).replaceAll("[\\[\\],]", "");
    }
}
