package recommender.content_based;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.Filenames;
import recommender.data_type.ProfileIdWritable;

import java.io.IOException;

public class Main {

    /**
     * Mapper Class
     * input: (key: filename, value: record (as MapWritable))
     * output: (key: artist (as ProfileIdWritable), value: tagId)
     */
    public static class ItemProfile1Mapper
            extends Mapper<Text, MapWritable, ProfileIdWritable, IntWritable>
    {
        @Override
        public void map(Text filename, MapWritable record, Context context) throws IOException {
            if (filename.toString().equalsIgnoreCase(Filenames.UT.get())) {
                ProfileIdWritable profileId = new ProfileIdWritable(false, record.get())
            }
        }
    }
    public static class CBReducer
            extends
            Reducer< Text, IntWritable, Text, IntWritable > {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce( Text key, Iterable < IntWritable > values, Context context
        ) throws IOException,
                InterruptedException {

        }
    }

    public static void main( String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job itemProfile1 = Job.getInstance( conf, "content-based recommender");

        itemProfile1.setJarByClass(Main.class);

        System.exit( itemProfile1.waitForCompletion( true) ? 0 : 1);
    }
}
