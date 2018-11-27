package recommender.content_based;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import recommender.file_record.LastfmFileInputFormat;
import recommender.hadoopext.io.ProfileAndTagWritable;
import recommender.hadoopext.io.ProfileIdWritable;

public class Main {

    public static void main( String[] args) throws Exception {
        // Item Profile Step 1
        Configuration conf = new Configuration();
        Job itemProfile1 = Job.getInstance( conf, "item profile step 1");
        itemProfile1.setJarByClass(ItemProfile.class);
        LastfmFileInputFormat.addInputPath(itemProfile1, new Path("input"));
        itemProfile1.setInputFormatClass(LastfmFileInputFormat.class);
        FileOutputFormat.setOutputPath(itemProfile1, new Path("itemProfile1"));
        itemProfile1.setMapperClass(ItemProfile.ItemProfile1Mapper.class);
        itemProfile1.setReducerClass(ItemProfile.ItemProfile1Reducer.class);
        itemProfile1.setMapOutputKeyClass(ProfileIdWritable.class);
        itemProfile1.setMapOutputValueClass(IntWritable.class);
        itemProfile1.setOutputKeyClass(ProfileAndTagWritable.class);
        itemProfile1.setOutputValueClass(IntWritable.class);

        System.exit( itemProfile1.waitForCompletion( true) ? 0 : 1);
    }

}
