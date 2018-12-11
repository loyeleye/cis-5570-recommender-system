package recommender.deprecated;

import org.apache.hadoop.mapreduce.Partitioner;
import recommender.hadoopext.io.ProfileFeatureWritable;

public class ProfileFeatureSortPartitioner extends Partitioner<ProfileFeatureWritable, NamedDoubleWritable> {

    @Override
    public int getPartition(ProfileFeatureWritable profileFeature, NamedDoubleWritable dw, int numPartitions) {
        return profileFeature.getProfile().hashCode() % numPartitions;
    }
}