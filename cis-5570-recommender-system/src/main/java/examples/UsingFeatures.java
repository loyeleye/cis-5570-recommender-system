package examples;

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
import recommender.content_based.Feature;
import recommender.hadoopext.io.*;

import java.io.IOException;

class UsingFeatures {
    /**
     * Given a list of user features: one for each individual <user,artist,feature> combination:
     * Get the average for each <user,feature> pair.
     */
    public static class TestFeatureMapper
            extends Mapper<Text, RecordWritable, ProfileFeatureWritable, AverageWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            if ("itemProfile".equalsIgnoreCase(record.getParentFolder().toString())) {
                try {
                    Feature itemProfileFeature = record.asFeature();
                    itemProfileFeature.ofArtist();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if ("userProfile".equalsIgnoreCase(record.getParentFolder().toString())) {
                try {
                    Feature userProfileFeature = record.asFeature();
                    userProfileFeature.isTag();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Given a list of user features: one for each individual <user,artist,feature> combination:
     * Get the average for each <user,feature> pair.
     */
    public static class TestFeatureReducer
            extends
            Reducer<ProfileFeatureWritable, AverageWritable, ProfileFeatureWritable, DoubleWritable> {

        public void reduce(ProfileFeatureWritable profileAndTag, Iterable<AverageWritable> values, Context context
        ) throws IOException,
                InterruptedException {
            AverageWritable average = new AverageWritable();
            for (AverageWritable value: values) {
                average.add(value);
            }
            context.write(profileAndTag, new DoubleWritable(average.getAverage()));
        }
    }

    public static void main( String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "testing features");

        job.setJarByClass(UsingFeatures.class);

        FileInputFormat.addInputPath( job, new Path("itemProfile"));
        FileInputFormat.addInputPath( job, new Path("userProfile"));
        FileOutputFormat.setOutputPath( job, new Path("testFeatures"));
        job.setMapperClass( TestFeatureMapper.class);
        job.setReducerClass(TestFeatureReducer.class);

        job.setOutputKeyClass( Text.class);
        job.setOutputValueClass( IntWritable.class);

        System.exit( job.waitForCompletion( true) ? 0 : 1);
    }
}
