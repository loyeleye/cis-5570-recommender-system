package recommender.hadoopext.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedJoiningGroupingComparator extends WritableComparator {


    public TaggedJoiningGroupingComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;
        return taggedKey1.getArtistIdToJoin().compareTo(taggedKey2.getArtistIdToJoin());
    }
}