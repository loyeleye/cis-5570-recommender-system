package recommender.fileformat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import recommender.deprecated.NamedDoubleWritable;
import recommender.deprecated.ProfileVectorWritable;
import recommender.hadoopext.io.ProfileIdWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexValueWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexVectorWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class VectorRecordReader extends RecordReader<ProfileIdWritable, ProfileVectorWritable> {
	private ProfileIdWritable key = new ProfileIdWritable();
	private ProfileVectorWritable value;

	private Text parentFolder = new Text();
	private BufferedReader in;
	private long start = 0;
	private long end = 0;
	private long pos = 0;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		FileSplit fileSplit = (FileSplit) inputSplit;
		start = fileSplit.getStart();
		pos = start;
		end = start + fileSplit.getLength();
		final Path file = fileSplit.getPath();
		parentFolder.set(file.getParent().getName());
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(file);
		in = new BufferedReader(new InputStreamReader(fileIn));
	}

	@Override
	public void close() throws IOException {
		in.close();

	}

	/**
	 * Returns current filename
	 * @return key current filename
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public ProfileIdWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	/**
	 * Returns current read line as writable
	 * @return record RecordWritable containing values of current read line
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public ProfileVectorWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// Read the next line
		String nextReadLine = in.readLine();

		// Return false if no more lines to read
		if (nextReadLine == null) return false;

		// Update the position marker and line #
		pos += nextReadLine.length();

		// Split values to an array
		String[] keyAndValue = StringUtils.split(nextReadLine, '\t');

		String[] k = StringUtils.split(keyAndValue[0], '-');
		key.setType(k[0].charAt(0)=='u');
		key.setId(Integer.parseInt(k[1]));

		List<NamedDoubleWritable> features = new ArrayList<>();

		for (String namedFeature: StringUtils.split(keyAndValue[1])) {
			String[] featureAndValue = StringUtils.split(namedFeature, ':');
			NamedDoubleWritable n = new NamedDoubleWritable(featureAndValue[0], Double.parseDouble(featureAndValue[1]));
			features.add(n);
		}

		value = new ProfileVectorWritable(features.toArray(new NamedDoubleWritable[]{}));

		return true;
	}
}
