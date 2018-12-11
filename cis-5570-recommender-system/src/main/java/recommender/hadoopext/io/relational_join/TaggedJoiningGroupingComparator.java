package recommender.hadoopext.io.relational_join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedJoiningGroupingComparator extends WritableComparator {


    public TaggedJoiningGroupingComparator() {
        super(RelationalJoinKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RelationalJoinKey relationalJoinKey1 = (RelationalJoinKey)a;
        RelationalJoinKey relationalJoinKey2 = (RelationalJoinKey)b;
        return relationalJoinKey1.getCommonIdToJoin().compareTo(relationalJoinKey2.getCommonIdToJoin());
    }
}