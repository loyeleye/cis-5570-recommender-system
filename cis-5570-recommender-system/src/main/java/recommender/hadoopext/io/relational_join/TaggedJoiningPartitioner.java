package recommender.hadoopext.io.relational_join;

import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedJoiningPartitioner extends Partitioner<RelationalJoinKey, UserProfileRelationJoinWritable> {

    @Override
    public int getPartition(RelationalJoinKey relationalJoinKey, UserProfileRelationJoinWritable dw, int numPartitions) {
        return relationalJoinKey.getCommonIdToJoin().hashCode() % numPartitions;
    }
}