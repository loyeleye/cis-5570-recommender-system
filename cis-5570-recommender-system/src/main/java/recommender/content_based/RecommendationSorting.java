package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.bloom.Key;
import recommender.enums.FileFolders;
import recommender.enums.Filenames;
import recommender.hadoopext.io.ProfileFeatureWritable;
import recommender.hadoopext.io.ProfileIdWritable;
import recommender.hadoopext.io.RecordWritable;
import recommender.hadoopext.io.cosine.KeyPair;
import recommender.hadoopext.io.recommendation.KeyPairSecondarySort;

import javax.net.ssl.KeyStoreBuilderParameters;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class RecommendationSorting {

    public static BloomFilter bloomFilter;

    public static boolean loadBloomFilter() {
        boolean success = (new File("bloomFilter/")).mkdirs();
        if (success) {
            FileReader in = null;
            try {
                in = new FileReader("input/user_artists.dat");
                BufferedReader br = new BufferedReader(in);
                String line;
                while ((line = br.readLine()) != null) {
                    bloomFilter = new BloomFilter(10000, 3, 1);
                    String[] userArtistWeight = br.readLine().split("\t");
                    String ua_pair = String.format("u%s,a%s", userArtistWeight[0], userArtistWeight[1]);
                    bloomFilter.add(new Key(ua_pair.getBytes()));
                    bloomFilter.not();
                    DataOutput output = new DataOutputStream(new FileOutputStream("bloomFilter/potentialRecFilter.dat"));
                    bloomFilter.write(output);
                }
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
                success = false;
            }
        }
        return success;
    }


    public static class TopNMapper
            extends Mapper<KeyPair, RecordWritable, KeyPairSecondarySort, DoubleWritable> {
        public void map(KeyPair key, RecordWritable record, Context context) throws IOException, InterruptedException {
            if (bloomFilter.membershipTest(new Key(key.toString().getBytes()))) {
                KeyPairSecondarySort kpss = new KeyPairSecondarySort(key, record.getScore().get());
                context.write(kpss, record.getScore());
            }
        }
    }

    public static class TopNReducer
            extends
            Reducer<KeyPairSecondarySort, DoubleWritable, Text, DoubleWritable > {
        MultipleOutputs mos;

        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        public void reduce( KeyPairSecondarySort key, Iterable < DoubleWritable > values, Context context) throws IOException,
                InterruptedException {
            DoubleWritable score;
            for (int i = 0; i < Main.N; i++) {
                if (!values.iterator().hasNext()) break;
                score = values.iterator().next();
                Text machineOutput = new Text(String.format("%d\t%d\t%d", key.getUserId(), key.getArtistId(), i+1));
                Text humanOutput = new Text(String.format("#%d) We recommend Artist %d to User %d >>> Recommendation Score:", i+1, key.getArtistId(), key.getUserId()));
                mos.write("recommendations", humanOutput, score, "output");
                context.write(machineOutput, score);
            }
        }
    }

}
