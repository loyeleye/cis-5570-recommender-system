package recommender.content_based;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.deprecated.ProfileVectorWritable;
import recommender.enums.FileFolders;
import recommender.hadoopext.io.*;
import recommender.hadoopext.io.cosine.KeyPair;
import recommender.hadoopext.io.cosine.ValuePair;
import recommender.hadoopext.io.inverted_index.InvertedIndexKeyWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexValueWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexVectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

class CosineSimilarity {
    public static class InvertedIndexMapper
            extends
            Mapper<Text, RecordWritable, InvertedIndexKeyWritable, InvertedIndexValueWritable> {
        static final String USER_PROFILE_TAGS = FileFolders.UP_TW.foldername();
        static final String ITEM_PROFILE_TAGS = FileFolders.IP_TW.foldername();
        static final String USER_PROFILE_PLAYS = FileFolders.UP_PC.foldername();
        static final String ITEM_PROFILE_PLAYS = FileFolders.IP_PC.foldername();

        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            try {
                String folderName = record.getParentFolder().toString();
                if (USER_PROFILE_TAGS.equalsIgnoreCase(folderName)) {
                    InvertedIndexKeyWritable key = new InvertedIndexKeyWritable(true, record.getTagId().get());
                    InvertedIndexValueWritable val = new InvertedIndexValueWritable("u"+record.getUserId().get(), record.getScore().get());
                    context.write(key, val);
                }
//                if (USER_PROFILE_PLAYS.equalsIgnoreCase(folderName)) {
//                    InvertedIndexKeyWritable key = new InvertedIndexKeyWritable(false, 0);
//                    InvertedIndexValueWritable val = new InvertedIndexValueWritable("u"+record.getUserId().get(), record.getScore().get());
//                    context.write(key, val);
//                }
                if (ITEM_PROFILE_TAGS.equalsIgnoreCase(folderName)) {
                    InvertedIndexKeyWritable key = new InvertedIndexKeyWritable(true, record.getTagId().get());
                    InvertedIndexValueWritable val = new InvertedIndexValueWritable("a"+record.getArtistId().get(), record.getScore().get());
                    context.write(key, val);
                }
//                if (ITEM_PROFILE_PLAYS.equalsIgnoreCase(folderName)) {
//                    InvertedIndexKeyWritable key = new InvertedIndexKeyWritable(false, 0);
//                    InvertedIndexValueWritable val = new InvertedIndexValueWritable("a"+record.getArtistId().get(), record.getScore().get());
//                    context.write(key, val);
//                }
            } catch (Exception e) {
                System.out.println(String.format("File failed on:\nfile %s\nparent folder %s\n record %s\nException: %s",
                        filename.toString(), record.getParentFolder(), record.toString(), e.getMessage()));
                throw new IOException(e);
            }

        }
    }

    public static class InvertedIndexReducer
            extends
            Reducer<InvertedIndexKeyWritable, InvertedIndexValueWritable, InvertedIndexKeyWritable, InvertedIndexVectorWritable> {
        public void reduce(InvertedIndexKeyWritable key, Iterable<InvertedIndexValueWritable> postings, Context context) throws IOException, InterruptedException {
            ArrayList<InvertedIndexValueWritable> vector = new ArrayList<>();
            for (InvertedIndexValueWritable posting : postings) {
                InvertedIndexValueWritable p = new InvertedIndexValueWritable(posting.getDocument(), posting.getValue());
                vector.add(p);
            }
            InvertedIndexVectorWritable writableVector = new InvertedIndexVectorWritable(vector.toArray(new InvertedIndexValueWritable[]{}));
            context.write(key, writableVector);
        }
    }

    public static class CosineMapper
            extends
            Mapper<IntWritable, InvertedIndexVectorWritable, KeyPair, ValuePair> {
        public void map(IntWritable tagId, InvertedIndexVectorWritable vector, Context context) throws IOException, InterruptedException {
            InvertedIndexValueWritable[] postings = vector.get();
            KeyPair key = new KeyPair();
            for (int i = 0; i < postings.length; i++) {
                InvertedIndexValueWritable postingA = postings[i];
                for (int j = i+1; j < postings.length; j++) {
                    InvertedIndexValueWritable postingB = postings[j];
                    if (postingA.getDocument().charAt(0) == postingB.getDocument().charAt(0)) continue;

                    key.parse(postingA.getDocument(), postingB.getDocument());
                    ValuePair value = new ValuePair(postingA.getValue(), postingB.getValue());

                    context.write(key, value);
                }
            }
        }
    }

    public static class CosineReducer
            extends
            Reducer<KeyPair, ValuePair, KeyPair, Double> {
        public void reduce(KeyPair key, Iterable<ValuePair> valuePairs, Context context) throws IOException, InterruptedException {
            double sim = 0;

            for (ValuePair valuePair: valuePairs) {
                sim += valuePair.getV1() * valuePair.getV2();
            }

            if (sim > Main.SIMILARITY_THRESHOLD) context.write(key, sim);
        }
    }
}
