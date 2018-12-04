package recommender.hadoopext.io;

import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedJoiningPartitioner extends Partitioner<JoinByArtistKey, UserProfileRelationJoinWritable> {

    @Override
    public int getPartition(JoinByArtistKey joinByArtistKey, UserProfileRelationJoinWritable dw, int numPartitions) {
        return joinByArtistKey.getArtistIdToJoin().hashCode() % numPartitions;
    }
}