package recommender.hadoopext.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedJoiningGroupingComparator extends WritableComparator {


    public TaggedJoiningGroupingComparator() {
        super(JoinByArtistKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinByArtistKey joinByArtistKey1 = (JoinByArtistKey)a;
        JoinByArtistKey joinByArtistKey2 = (JoinByArtistKey)b;
        return joinByArtistKey1.getArtistIdToJoin().compareTo(joinByArtistKey2.getArtistIdToJoin());
    }
}