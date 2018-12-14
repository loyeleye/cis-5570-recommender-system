package recommender.content_based;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import recommender.hadoopext.io.RecordWritable;
import recommender.hadoopext.io.cosine.KeyPair;
import recommender.hadoopext.io.recommendation.KeyPairSecondarySort;

import java.io.*;

class RecommendationSorting {

    public static class TopNMapper
            extends Mapper<KeyPair, RecordWritable, KeyPairSecondarySort, DoubleWritable> {
        static BloomFilter bloomFilter;

        public void setup(Context context) {
            FileReader in = null;
            String line;
            int n = Main.NUM_USER_ARTIST_RECORDS; // # of records in user_artists.dat
            int m = getM(n, Main.FALSE_POSITIVE_RATE);
            int k = getK(n, m);
            try {
                in = new FileReader("input/user_artists.dat");
                BufferedReader br = new BufferedReader(in);
                bloomFilter = new BloomFilter(m, k, 1);
                while ((line = br.readLine()) != null) {
                    String[] userArtistWeight = line.split("\t");
                    String ua_pair = String.format("u%s,a%s", userArtistWeight[0], userArtistWeight[1]);
                    bloomFilter.add(new Key(ua_pair.getBytes()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // https://corte.si/posts/code/bloom-filter-rules-of-thumb/index.html
        public static int getM(int n,
                               float fpr) {
            return (int) (Math.log(1.0/fpr) / Math.pow(Math.log(2),2)) * n;
        }

        public static int getK(float n, float m) {
            return (int) (m * Math.log(2) / n);
        }

        public void map(KeyPair key, RecordWritable record, Context context) throws IOException, InterruptedException {
            if (!bloomFilter.membershipTest(new Key(key.toString().getBytes()))) {
                KeyPairSecondarySort kpss = new KeyPairSecondarySort(key, record.getScore().get());
                context.write(kpss, record.getScore());
            }
        }
    }

    public static class TopNReducer
            extends
            Reducer<KeyPairSecondarySort, DoubleWritable, Text, DoubleWritable > {
        MultipleOutputs<Text, DoubleWritable> mos;

        public void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        public void reduce( KeyPairSecondarySort key, Iterable < DoubleWritable > values, Context context) throws IOException,
                InterruptedException {
            DoubleWritable score;
            for (int i = 0; i < Main.NUM_RECOMMENDATIONS; i++) {
                if (!values.iterator().hasNext()) break;
                score = values.iterator().next();
                // userId artist rank
                Text machineOutput = new Text(String.format("%d\t%d\t%d", key.getUserId(), key.getArtistId(), i+1));
                if (Main.DESCRIPTIVE) {
                    Text humanOutput = new Text(String.format("#%d) Recommend Artist %d to User %d >>> Recommendation Score:", i+1, key.getArtistId(), key.getUserId()));
                    mos.write(humanOutput, score, "recommendations/" + key.getUserId());
                }
                context.write(machineOutput, score);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException { mos.close(); }
    }

}
