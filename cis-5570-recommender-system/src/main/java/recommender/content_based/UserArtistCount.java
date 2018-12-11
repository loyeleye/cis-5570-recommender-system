package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import recommender.enums.Filenames;
import recommender.hadoopext.io.RecordWritable;

import java.io.IOException;

class UserArtistCount {
    public static class ArtistCountsPerUserMapper
            extends
            Mapper<Text, RecordWritable, IntWritable, IntWritable> {
        public void map(Text filename, RecordWritable record, Context context) throws IOException, InterruptedException {
            if (Filenames.UA.filename().equalsIgnoreCase(filename.toString())) {
                context.write(new IntWritable(record.getUserId().get()), new IntWritable(1));
            }
        }
    }

    public static class ArtistCountsPerUserReducer
            extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable userId, Iterable<IntWritable> artistCounts, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            for (IntWritable cnt : artistCounts) {
                totalCount += cnt.get();
            }
            context.write(userId, new IntWritable(totalCount));
        }
    }
}
