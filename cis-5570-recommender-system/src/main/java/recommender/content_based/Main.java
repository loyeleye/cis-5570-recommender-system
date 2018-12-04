package recommender.content_based;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import recommender.fileformat.LastfmFileInputFormat;
import recommender.hadoopext.io.*;

public class Main {

    public static void main( String[] args) throws Exception {
        boolean success;
        Configuration conf = new Configuration();
        // Item Profile Weight Averaging
        Job itemProfile1 = Job.getInstance( conf, "item profile weights");
        itemProfile1.setJarByClass(ItemProfile.class);
        LastfmFileInputFormat.addInputPath(itemProfile1, new Path("input"));
        itemProfile1.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(itemProfile1, new Path("itemProfilePC"));
        itemProfile1.setMapperClass(ItemProfile.ItemProfileWeightMapper.class);
        itemProfile1.setReducerClass(ItemProfile.ItemProfileWeightReducer.class);
        itemProfile1.setMapOutputKeyClass(ProfileIdWritable.class);
        itemProfile1.setMapOutputValueClass(IntWritable.class);
        itemProfile1.setOutputKeyClass(ProfileIdWritable.class);
        itemProfile1.setOutputValueClass(DoubleWritable.class);
        itemProfile1.waitForCompletion(true);
        // User Profile Weight Averaging
        Job userProfile1 = Job.getInstance(conf, "user profile weights");
        userProfile1.setJarByClass(UserProfile.class);
        LastfmFileInputFormat.addInputPath(userProfile1, new Path("input"));
        userProfile1.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(userProfile1, new Path("userProfilePC"));
        userProfile1.setMapperClass(UserProfile.UserProfileWeightMapper.class);
        userProfile1.setReducerClass(UserProfile.UserProfileWeightReducer.class);
        userProfile1.setMapOutputKeyClass(ProfileIdWritable.class);
        userProfile1.setMapOutputValueClass(AverageWritable.class);
        userProfile1.setOutputKeyClass(ProfileIdWritable.class);
        userProfile1.setOutputValueClass(DoubleWritable.class);
        userProfile1.waitForCompletion(true);
        // Item Profile
        Job itemProfile = Job.getInstance( conf, "item profile");
        itemProfile.setJarByClass(ItemProfile.class);
        LastfmFileInputFormat.addInputPath(itemProfile, new Path("input"));
        itemProfile.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(itemProfile, new Path("itemProfile"));
        itemProfile.setMapperClass(ItemProfile.ItemProfileMapper.class);
        itemProfile.setReducerClass(ItemProfile.ItemProfileReducer.class);
        itemProfile.setMapOutputKeyClass(ProfileIdWritable.class);
        itemProfile.setMapOutputValueClass(IntWritable.class);
        itemProfile.setOutputKeyClass(ProfileFeatureWritable.class);
        itemProfile.setOutputValueClass(DoubleWritable.class);
        success = itemProfile.waitForCompletion( true);
        // User Profile Join
        Job userArtistJoin = Job.getInstance( conf, "user profile join");
        userArtistJoin.setJarByClass(UserProfile.class);
        LastfmFileInputFormat.addInputPath(userArtistJoin, new Path("itemProfile"));
        LastfmFileInputFormat.addInputPath(userArtistJoin, new Path("input"));
        userArtistJoin.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(userArtistJoin, new Path("userProfJoin"));
        userArtistJoin.setMapperClass(UserProfile.UserProfileRelationJoinMapper.class);
        userArtistJoin.setReducerClass(UserProfile.UserProfileRelationJoinReducer.class);
        userArtistJoin.setPartitionerClass(TaggedJoiningPartitioner.class);
        userArtistJoin.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        userArtistJoin.setMapOutputKeyClass(JoinByArtistKey.class);
        userArtistJoin.setMapOutputValueClass(UserProfileRelationJoinWritable.class);
        userArtistJoin.setOutputKeyClass(ProfileFeatureWritable.class);
        userArtistJoin.setOutputValueClass(DoubleWritable.class);
        success = success && userArtistJoin.waitForCompletion( true);
        // User Profile Aggregate
        Job userProfileAgg = Job.getInstance(conf, "user profile agg");
        userProfileAgg.setJarByClass(UserProfile.class);
        LastfmFileInputFormat.addInputPath(userProfileAgg, new Path("userProfJoin"));
        userProfileAgg.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(userProfileAgg, new Path("userProfile"));
        userProfileAgg.setMapperClass(UserProfile.UserProfileTagWeightAveragingMapper.class);
        userProfileAgg.setReducerClass(UserProfile.UserProfileTagWeightAveragingReducer.class);
        userProfileAgg.setMapOutputKeyClass(ProfileFeatureWritable.class);
        userProfileAgg.setMapOutputValueClass(AverageWritable.class);
        userProfileAgg.setOutputKeyClass(ProfileFeatureWritable.class);
        userProfileAgg.setOutputValueClass(DoubleWritable.class);
        success = success && userProfileAgg.waitForCompletion( true);
        success = success && itemProfile1.waitForCompletion(true) && userProfile1.waitForCompletion(true);
        // End Process
        System.exit(success ? 0 : 1);
    }

}
