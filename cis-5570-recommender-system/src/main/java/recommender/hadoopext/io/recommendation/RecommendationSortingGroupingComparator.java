package recommender.hadoopext.io.recommendation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import recommender.hadoopext.io.relational_join.RelationalJoinKey;

public class RecommendationSortingGroupingComparator extends WritableComparator {


    public RecommendationSortingGroupingComparator() {
        super(KeyPairSecondarySort.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyPairSecondarySort kp1 = (KeyPairSecondarySort)a;
        KeyPairSecondarySort kp2 = (KeyPairSecondarySort)b;
        return kp1.getUserId().compareTo(kp2.getUserId());
    }
}