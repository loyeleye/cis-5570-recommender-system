package recommender.hadoopext.io.recommendation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import recommender.hadoopext.io.relational_join.RelationalJoinKey;
import recommender.hadoopext.io.relational_join.UserProfileRelationJoinWritable;

public class RecommendationSortingPartitioner extends Partitioner<KeyPairSecondarySort, DoubleWritable> {

    @Override
    public int getPartition(KeyPairSecondarySort kpSort, DoubleWritable dw, int numPartitions) {
        return kpSort.getUserId().hashCode() % numPartitions;
    }
}