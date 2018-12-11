package recommender.hadoopext.io.inverted_index;

import org.apache.hadoop.io.ArrayWritable;
import recommender.deprecated.NamedDoubleWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexValueWritable;

import java.util.Arrays;

public class InvertedIndexVectorWritable extends ArrayWritable {
    public InvertedIndexVectorWritable(InvertedIndexValueWritable[] values) {
        super(InvertedIndexValueWritable.class, values);
    }

    @Override
    public InvertedIndexValueWritable[] get() {
        return (InvertedIndexValueWritable[]) super.get();
    }

    @Override
    public String toString() {
        return Arrays.toString(get()).replaceAll("[\\[\\],]", "");
    }
}
