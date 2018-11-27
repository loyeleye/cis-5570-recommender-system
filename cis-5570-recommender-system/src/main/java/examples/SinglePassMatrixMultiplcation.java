package examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SinglePassMatrixMultiplcation {
	// bit indicators for matrix M and N
	private final static boolean M = true;
	private final static boolean N = false;
	
	public static class MatrixMultiplicationMapper
			extends
			Mapper<MapKeyIn, Text, Text, Text> {

		@Override
		public void map(MapKeyIn keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException {
			MapAndReduceKeyOut keyOut;
			MapValueOut valueOut;
			String rowString = valueIn.toString();
			String[] rows = rowString.split("\\s+");
		
			if (keyIn.getMatrixBitIndicator() == M) {
				for (int j = 1; j <= rows.length; j++) {
					Integer m_ij = Integer.parseInt(rows[j - 1]);
					for (int k = 1; k <= keyIn.getMaxK(); k++) {
						keyOut = new MapAndReduceKeyOut(keyIn.getCurrentI(), k);
						valueOut = new MapValueOut(M, j, m_ij);
						context.write(keyOut.toText(), valueOut.toText());
					}
				}
			} else if (keyIn.getMatrixBitIndicator() == N) {
				for (int k = 1; k <= rows.length; k++) {
					Integer n_jk = Integer.parseInt(rows[k - 1]);
					for (int i = 1; i <= keyIn.getMaxI(); i++) {
						keyOut = new MapAndReduceKeyOut(i, k);
						valueOut = new MapValueOut(N, keyIn.getCurrentJ(), n_jk);
						context.write(keyOut.toText(), valueOut.toText());
					}
				}
			}
		}
	}

	public static class MatrixMultiplicationReducer
			extends
			Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(
				Text key,
				Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			
			// Key is (matrix bit indicator, j), Value is product
			Map<Integer, Long> products = new HashMap<Integer, Long>();
			Long sum_of_products = 0L;
			Text value = new Text();
			
			for (Text textVal: values) {
				MapValueOut val = new MapValueOut(textVal.toString());
				
				if (val.hasError()) continue;
				
				if (products.containsKey(val.getjValue())) {
					Long new_product = products.get(val.getjValue()) * val.getCellValue();
					products.put(val.getjValue(), new_product);
				} else {
					products.put(val.getjValue(), val.getCellValue().longValue());
				}
			}
			
			for (Long product: products.values()) 
				sum_of_products += product;
			
			value.set(sum_of_products.toString());
			
			context.write(key, value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Single pass matrix multiplication");
		

		job.setJarByClass(SinglePassMatrixMultiplcation.class);

		MatrixFileInputFormat.addInputPath(job, new Path("input"));
		job.setInputFormatClass(MatrixFileInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.setMapperClass(MatrixMultiplicationMapper.class);
		job.setReducerClass(MatrixMultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
