package recommender.content_based;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import recommender.fileformat.CosineOutputFileInputFormat;
import recommender.fileformat.InvertedIndexFileInputFormat;
import recommender.fileformat.LastfmFileInputFormat;
import recommender.hadoopext.io.ProfileFeatureWritable;
import recommender.hadoopext.io.ProfileIdWritable;
import recommender.hadoopext.io.cosine.KeyPair;
import recommender.hadoopext.io.cosine.ValuePair;
import recommender.hadoopext.io.inverted_index.InvertedIndexKeyWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexValueWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexVectorWritable;
import recommender.hadoopext.io.recommendation.KeyPairSecondarySort;
import recommender.hadoopext.io.recommendation.RecommendationSortingGroupingComparator;
import recommender.hadoopext.io.recommendation.RecommendationSortingPartitioner;
import recommender.hadoopext.io.relational_join.RelationalJoinKey;
import recommender.hadoopext.io.relational_join.TaggedJoiningGroupingComparator;
import recommender.hadoopext.io.relational_join.TaggedJoiningPartitioner;
import recommender.hadoopext.io.relational_join.UserProfileRelationJoinWritable;

import java.io.IOException;

import static recommender.enums.FileFolders.*;

public class Main {
    static final Double SIMILARITY_THRESHOLD = 0.6;
    static final int NUM_USER_ARTIST_RECORDS = 92835 - 1;
    static final float FALSE_POSITIVE_RATE = 0.0005f;

    static Integer NUM_RECOMMENDATIONS = 10;
    static final Boolean DESCRIPTIVE = true;

    private static Job createNewJob(String jobName, Class jobClass, String[] ins, String out, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass,
                            Class mapKeyOut, Class mapValueOut, Class reduceKeyOut, Class reduceValueOut) throws IOException {
        Configuration conf = new Configuration();
        Job newJob = Job.getInstance(conf, jobName);
        newJob.setJarByClass(jobClass);
        for (String in: ins) {
            LastfmFileInputFormat.addInputPath(newJob, new Path(in));
        }
        newJob.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(newJob, new Path(out));
        newJob.setMapperClass(mapperClass);
        newJob.setReducerClass(reducerClass);
        newJob.setMapOutputKeyClass(mapKeyOut);
        newJob.setMapOutputValueClass(mapValueOut);
        newJob.setOutputKeyClass(reduceKeyOut);
        newJob.setOutputValueClass(reduceValueOut);
        return newJob;
    }

    private static Job createNewJob(String jobName, Class jobClass, String in , String out, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass,
                                    Class mapKeyOut, Class mapValueOut, Class reduceKeyOut, Class reduceValueOut) throws IOException {
        return createNewJob(jobName, jobClass, new String[]{in}, out, mapperClass, reducerClass, mapKeyOut, mapValueOut, reduceKeyOut, reduceValueOut);
    }

    private static Job createJobUsingCustomFIF(String jobName, Class jobClass, String[] ins, String out, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass,
                                               Class mapKeyOut, Class mapValueOut, Class reduceKeyOut, Class reduceValueOut, FileInputFormat fif) throws IOException {
        Configuration conf = new Configuration();
        Job newJob = Job.getInstance(conf, jobName);
        newJob.setJarByClass(jobClass);
        for (String in: ins) {
            fif.addInputPath(newJob, new Path(in));
        }
        newJob.setInputFormatClass(fif.getClass());
        FileOutputFormat.setOutputPath(newJob, new Path(out));
        newJob.setMapperClass(mapperClass);
        newJob.setReducerClass(reducerClass);
        newJob.setMapOutputKeyClass(mapKeyOut);
        newJob.setMapOutputValueClass(mapValueOut);
        newJob.setOutputKeyClass(reduceKeyOut);
        newJob.setOutputValueClass(reduceValueOut);
        return newJob;
    }

    public static void main( String[] args) throws Exception {
        NUM_RECOMMENDATIONS = (args.length > 0) ? Integer.parseInt(args[0]) : 10;
        System.out.printf("***# of Recommendations set to %d", NUM_RECOMMENDATIONS);

        boolean success;

        // User Artist Counter
        Job userArtistCounter = createNewJob("user artist cnt", UserArtistCount.class, INPUT.foldername(), UA_CT.foldername(),
                UserArtistCount.ArtistCountsPerUserMapper.class, UserArtistCount.ArtistCountsPerUserReducer.class, IntWritable.class, IntWritable.class,
                IntWritable.class, IntWritable.class);
        userArtistCounter.submit();

        // Item Profile
        Job itemProfileGeneration = createNewJob("item profile", ItemProfile.class, INPUT.foldername(), IP_TW.foldername(),
                ItemProfile.ItemProfileMapper.class, ItemProfile.ItemProfileReducer.class, ProfileIdWritable.class, IntWritable.class,
                ProfileFeatureWritable.class, DoubleWritable.class);
        success = itemProfileGeneration.waitForCompletion( true);

        // User Artists Join Item Profile on Artist ID
        Job userArtistItemProfileJoin = createNewJob("user artist item prof join", UserProfile.class, new String[] {IP_TW.foldername(), INPUT.foldername()}, UP_JN.foldername(),
                UserProfile.UserProfileRelationJoinMapper.class, UserProfile.UserProfileRelationJoinReducer.class, RelationalJoinKey.class, UserProfileRelationJoinWritable.class,
                ProfileFeatureWritable.class, DoubleWritable.class);
        userArtistItemProfileJoin.setPartitionerClass(TaggedJoiningPartitioner.class);
        userArtistItemProfileJoin.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        success = success && userArtistItemProfileJoin.waitForCompletion( true);

        // User Profile Tag Score Aggregator
        Job userProfileTagScoreAggregator = createNewJob("user profile tag wt agg", UserProfile.class, UP_JN.foldername(), UP_NN.foldername(),
                UserProfile.UserProfileTagWeightAggregatorMapper.class, UserProfile.UserProfileTagWeightAggregatorReducer.class, ProfileFeatureWritable.class, DoubleWritable.class,
                ProfileFeatureWritable.class, DoubleWritable.class);
        success = success && userProfileTagScoreAggregator.waitForCompletion( true);
        success = success && userArtistCounter.waitForCompletion(true);

        // User Profile Tag Score Normalizer
        Job userProfile = createNewJob("user profile", UserProfile.class, new String[] {UP_NN.foldername(), UA_CT.foldername()}, UP_TW.foldername(),
                UserProfile.UserProfileTagWeightNormalizedMapper.class, UserProfile.UserProfileTagWeightNormalizedReducer.class, RelationalJoinKey.class, UserProfileRelationJoinWritable.class,
                ProfileFeatureWritable.class, DoubleWritable.class);
        userProfile.setPartitionerClass(TaggedJoiningPartitioner.class);
        userProfile.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        success = success && userProfile.waitForCompletion(true);

        // Create Inverted Index
        Job invertedIndex = createNewJob("inverted index", CosineSimilarity.class, new String[] {IP_TW.foldername(),  UP_TW.foldername()}, INDEX.foldername(),
                CosineSimilarity.InvertedIndexMapper.class, CosineSimilarity.InvertedIndexReducer.class, InvertedIndexKeyWritable.class, InvertedIndexValueWritable.class,
                InvertedIndexKeyWritable.class, InvertedIndexVectorWritable.class);
        success = success && invertedIndex.waitForCompletion(true);

        // Do Cosine Similarity
        Job cosineSimilarity = createJobUsingCustomFIF("cosine similarity", CosineSimilarity.class, new String[] {INDEX.foldername()}, COSSIM.foldername(),
                CosineSimilarity.CosineMapper.class, CosineSimilarity.CosineReducer.class, KeyPair.class, ValuePair.class,
                KeyPair.class, DoubleWritable.class, new InvertedIndexFileInputFormat());
        success = success && cosineSimilarity.waitForCompletion(true);

        // Get Sorted Recommendations
        Job getTopNRecommendations = createJobUsingCustomFIF("top n recommends", RecommendationSorting.class, new String[] {COSSIM.foldername()}, OUTPUT.foldername(),
                RecommendationSorting.TopNMapper.class, RecommendationSorting.TopNReducer.class, KeyPairSecondarySort.class, DoubleWritable.class,
                Text.class, DoubleWritable.class, new CosineOutputFileInputFormat());
        getTopNRecommendations.setPartitionerClass(RecommendationSortingPartitioner.class);
        getTopNRecommendations.setGroupingComparatorClass(RecommendationSortingGroupingComparator.class);
        success = success && getTopNRecommendations.waitForCompletion(true);

        // End Process
        System.exit(success ? 0 : 1);
    }
}
