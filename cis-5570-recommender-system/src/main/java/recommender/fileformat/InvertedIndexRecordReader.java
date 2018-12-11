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
import recommender.hadoopext.io.RecordWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexValueWritable;
import recommender.hadoopext.io.inverted_index.InvertedIndexVectorWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static recommender.enums.Filenames.UA;
import static recommender.enums.Filenames.UT;

public class InvertedIndexRecordReader extends RecordReader<IntWritable, InvertedIndexVectorWritable> {
	private IntWritable key = new IntWritable();

	private Text parentFolder = new Text();
	private InvertedIndexVectorWritable value;
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
		key.set(-1);
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
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	/**
	 * Returns current read line as writable
	 * @return record RecordWritable containing values of current read line
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public InvertedIndexVectorWritable getCurrentValue() throws IOException, InterruptedException {
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

		key.set(Integer.parseInt(keyAndValue[0]));
		List<InvertedIndexValueWritable> postings = new ArrayList<>();

		for (String posting: StringUtils.split(keyAndValue[1])) {
			String[] valueAndDoc = StringUtils.split(posting, '@');
			InvertedIndexValueWritable i = new InvertedIndexValueWritable(valueAndDoc[1], Double.parseDouble(valueAndDoc[0]));
			postings.add(i);
		}

		value = new InvertedIndexVectorWritable(postings.toArray(new InvertedIndexValueWritable[]{}));

		return true;
	}
}
