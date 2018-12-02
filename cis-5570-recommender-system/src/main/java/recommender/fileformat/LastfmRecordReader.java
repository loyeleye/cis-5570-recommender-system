package recommender.fileformat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import recommender.hadoopext.io.RecordWritable;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static recommender.enums.Filenames.UA;
import static recommender.enums.Filenames.UT;

public class LastfmRecordReader extends RecordReader<Text, RecordWritable> {
	private Text key = new Text();

	private Text parentFolder = new Text();
	private RecordWritable record = new RecordWritable();
	private BufferedReader in;
	private long start = 0;
	private long end = 0;
	private long pos = 0;
	private long line = 0;
	private int record_schema_length = 0;

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
		key.set(file.getName());

		// Read the first line as the record_schema
		String sch = in.readLine();
		// Update the position marker and line #
		pos += sch.length();
		line++;
		// Get the record schema length
		record_schema_length = StringUtils.split(sch).length;
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
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	/**
	 * Returns current read line as writable
	 * @return record RecordWritable containing values of current read line
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public RecordWritable getCurrentValue() throws IOException, InterruptedException {
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

		// Update the position marker and line #
		pos += nextReadLine.length();
		line++;

		// Split values to an array
		String[] nextReadLineValues = StringUtils.split(nextReadLine);

		if (record_schema_length != nextReadLineValues.length) {
			// Values don't match up to record_schema. Throw IO error
			throw new IOException(
					String.format("ERROR: Values do not match up to record_schema in file %s at line %d. Schema size: %d, Record size: %d", key, line, record_schema_length, nextReadLineValues.length)
			);
		}

		// Assign read line values to the record writable, depending on file type
		if (UT.filename().equalsIgnoreCase(key.toString())) {
			record = RecordWritable.readUserTaggedArtist(nextReadLineValues, parentFolder);
			record.setParentFolder(this.parentFolder);
		} else if (UA.filename().equalsIgnoreCase(key.toString())) {
			record = RecordWritable.readUserArtist(nextReadLineValues, parentFolder);
			record.setParentFolder(this.parentFolder);
		} else if ("itemProfile".equalsIgnoreCase(parentFolder.toString())) {
			record = RecordWritable.readItemProfile(nextReadLineValues, parentFolder);
			record.setParentFolder(this.parentFolder);
		} else {
			record = RecordWritable.readOther(nextReadLineValues);
		}

		return true;
	}
}
