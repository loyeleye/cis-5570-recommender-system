package recommender.deprecated;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import recommender.hadoopext.io.ProfileFeatureWritable;

public class ProfileFeatureSortGroupingComparator extends WritableComparator {


    public ProfileFeatureSortGroupingComparator() {
        super(ProfileFeatureWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ProfileFeatureWritable profileFeature1 = (ProfileFeatureWritable) a;
        ProfileFeatureWritable profileFeature2 = (ProfileFeatureWritable) b;
        return profileFeature1.getProfile().compareTo(profileFeature2.getProfile());
    }
}