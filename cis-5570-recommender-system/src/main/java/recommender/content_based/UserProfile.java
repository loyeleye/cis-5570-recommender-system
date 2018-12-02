package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.enums.Filenames;
import recommender.hadoopext.io.ProfileAndTagWritable;
import recommender.hadoopext.io.RecordWritable;
import recommender.hadoopext.io.RelationJoinValueWritable;
import recommender.hadoopext.io.TaggedKey;


import java.io.IOException;
import java.util.ArrayList;

public class UserProfile {
    /**
     * Mapper Class
     * T
     * Takes the user_taggedartists file and generates (artist, tag) pairs
     * input: (key: filename, value: record (as RecordWritable))
     * output: (key: artistProfile (as a ProfileIdWritable), value: tagId)
     */
    public static class JoinMapper
            extends Mapper<Text, RecordWritable, TaggedKey, RelationJoinValueWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {

            if (Filenames.UA.filename().equalsIgnoreCase(filename.toString())) {
                TaggedKey myTaggedKey = new TaggedKey(record.getArtistId(), filename);
                DoubleWritable weight = new DoubleWritable();
                weight.set(record.getWeight().get());
                RelationJoinValueWritable outputValues = new RelationJoinValueWritable();
                outputValues.setRelationTable(filename);
                outputValues.setTagWeight(weight);
                // Write output to file
                context.write(myTaggedKey, outputValues);
            }
            if ("itemProfile".equalsIgnoreCase(record.getParentFolder().toString())) {
                RelationJoinValueWritable outputValues = new RelationJoinValueWritable();
                TaggedKey myTaggedKey = new TaggedKey(record.getArtistId(), filename);
                outputValues.setRelationTable(filename);
                outputValues.setTagWeight(record.getTagWeight());
                // Write output to file
                context.write(myTaggedKey, outputValues);
            }
        }
    }

    /**
     * Reducer Class
     * Gets a percentage tag score for all tags associated with a particular artist
     * input: (key: artistProfile (as a ProfileIdWritable), value: tagId)
     * output: (key: (artistProfile,tagId), value: artist-tag-score)
     */
    public static class JoinReducer
            extends
            Reducer<TaggedKey, RelationJoinValueWritable, ProfileAndTagWritable, DoubleWritable> {

        private DoubleWritable artistTagScore = new DoubleWritable();
        private ProfileAndTagWritable profileAndTag = new ProfileAndTagWritable();

        public void reduce(TaggedKey key, Iterable<RelationJoinValueWritable> values, Context context
        ) throws IOException,
                InterruptedException {

            ArrayList<DoubleWritable> firstFile = new ArrayList<DoubleWritable>();
            Text file = key.getFilenameSource();
            //TextPair value = null;
            for (RelationJoinValueWritable value : values) {
                continue;
//                if(key..compareTo(tag)==0)
//                {
//                    firstFile.add(value.);
//                }
//                else
//                {
//                    for(Text val : T1)
//                    {
//                        output.collect(key.getFirst (),
//                                new Text(val.toString () + "\t"
//                                        + value.getFirst ().toString ()));
//                    }
            }
        }

    }
}
