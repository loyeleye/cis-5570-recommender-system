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
        // Item Profile
        Configuration conf = new Configuration();
        Job itemProfile = Job.getInstance( conf, "item profile");
        itemProfile.setJarByClass(ItemProfile.class);
        LastfmFileInputFormat.addInputPath(itemProfile, new Path("input"));
        itemProfile.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(itemProfile, new Path("itemProfile"));
        itemProfile.setMapperClass(ItemProfile.ItemProfileMapper.class);
        itemProfile.setReducerClass(ItemProfile.ItemProfileReducer.class);
        itemProfile.setMapOutputKeyClass(ProfileIdWritable.class);
        itemProfile.setMapOutputValueClass(IntWritable.class);
        itemProfile.setOutputKeyClass(ProfileAndTagWritable.class);
        itemProfile.setOutputValueClass(DoubleWritable.class);
        success = itemProfile.waitForCompletion( true);
        // User Profile Join
        Job userArtistJoin = Job.getInstance( conf, "user profile join");
        userArtistJoin.setJarByClass(UserProfile.class);
        LastfmFileInputFormat.addInputPath(userArtistJoin, new Path("itemProfile"));
        LastfmFileInputFormat.addInputPath(userArtistJoin, new Path("input"));
        userArtistJoin.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(userArtistJoin, new Path("userProfJoin"));
        userArtistJoin.setMapperClass(UserProfile.UserProfileJoinMapper.class);
        userArtistJoin.setReducerClass(UserProfile.UserProfileJoinReducer.class);
        userArtistJoin.setPartitionerClass(TaggedJoiningPartitioner.class);
        userArtistJoin.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        userArtistJoin.setMapOutputKeyClass(TaggedKey.class);
        userArtistJoin.setMapOutputValueClass(RelationJoinValueWritable.class);
        userArtistJoin.setOutputKeyClass(ProfileAndTagWritable.class);
        userArtistJoin.setOutputValueClass(DoubleWritable.class);
        success = userArtistJoin.waitForCompletion( true) && success;
        // User Profile Aggregate
        Job userProfileAgg = Job.getInstance(conf, "user profile agg");
        userProfileAgg.setJarByClass(UserProfile.class);
        LastfmFileInputFormat.addInputPath(userProfileAgg, new Path("userProfJoin"));
        userProfileAgg.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(userProfileAgg, new Path("userProfile"));
        userProfileAgg.setMapperClass(UserProfile.UserProfileAggregateMapper.class);
        userProfileAgg.setReducerClass(UserProfile.UserProfileAggregateReducer.class);
        userProfileAgg.setMapOutputKeyClass(ProfileAndTagWritable.class);
        userProfileAgg.setMapOutputValueClass(DoubleWritable.class);
        userProfileAgg.setOutputKeyClass(ProfileAndTagWritable.class);
        userProfileAgg.setOutputValueClass(DoubleWritable.class);
        success = userProfileAgg.waitForCompletion( true) && success;
        // End Process
        System.exit(success ? 0 : 1);
    }

}
