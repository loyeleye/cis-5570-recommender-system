package recommender.hadoopext.io;

import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, RelationJoinValueWritable> {

    @Override
    public int getPartition(TaggedKey taggedKey, RelationJoinValueWritable dw, int numPartitions) {
        return taggedKey.getArtistIdToJoin().hashCode() % numPartitions;
    }
}