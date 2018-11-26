package recommender.file_record;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LastfmRecordReader extends RecordReader<Text, MapWritable> {
	private Text key = new Text();
	private MapWritable record = new MapWritable();
	private ArrayWritable record_schema;
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
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(file);
		in = new BufferedReader(new InputStreamReader(fileIn));
		key.set(file.getName());

		// Read the first line as the record_schema
		String sch = in.readLine();
		// Update the position marker
		pos += sch.length();
		// Store the record_schema for this file
		record_schema = new ArrayWritable(StringUtils.split(sch));
	}

	@Override
	public void close() throws IOException {
		in.close();

	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public MapWritable getCurrentValue() throws IOException, InterruptedException {
		return record;
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

		// Update the position marker
		pos += nextReadLine.length();

		// Split values to an array
		String[] nextReadLineValues = StringUtils.split(nextReadLine);

		if (record_schema.get().length != nextReadLineValues.length) {
			// Values don't match up to record_schema. Throw IO error
			throw new IOException(
					String.format("ERROR: Values do not match up to record_schema in file %s at pos %d", key, pos)
			);
		}

		// Assign read values to an array writable
		ArrayWritable record_values = new ArrayWritable(nextReadLineValues);

		// Clear the current record values
		record.clear();

		// Assign the new record values
		for (int i = 0; i < record_schema.get().length; i++) {
			record.put(record_schema.get()[i], record_values.get()[i]);
		}

		return true;
	}
}
