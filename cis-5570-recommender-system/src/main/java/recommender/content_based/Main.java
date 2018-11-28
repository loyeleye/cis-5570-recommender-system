package recommender.content_based;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import recommender.fileformat.LastfmFileInputFormat;
import recommender.hadoopext.io.ProfileAndTagWritable;
import recommender.hadoopext.io.ProfileIdWritable;

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
        // User Profile
        Job userProfile = Job.getInstance( conf, "user profile");
        userProfile.setJarByClass(ItemProfile.class);
        LastfmFileInputFormat.addInputPath(userProfile, new Path("input"));
        userProfile.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(userProfile, new Path("userProfile"));
        userProfile.setMapperClass(ItemProfile.ItemProfileMapper.class);
        userProfile.setReducerClass(ItemProfile.ItemProfileReducer.class);
        userProfile.setMapOutputKeyClass(ProfileIdWritable.class);
        userProfile.setMapOutputValueClass(IntWritable.class);
        userProfile.setOutputKeyClass(ProfileAndTagWritable.class);
        userProfile.setOutputValueClass(DoubleWritable.class);
        success = userProfile.waitForCompletion( true) && success;
        System.exit(success ? 0 : 1);
    }

}
