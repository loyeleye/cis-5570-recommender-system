package examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatrixRecordReader extends RecordReader<MapKeyIn, Text> {
	// bit indicators for matrix M and N
	public final static boolean M = true;
	public final static boolean N = false;
	
	private BufferedReader in;
	private Boolean currentMatrix = M;
	private Integer maxI = 0;
	private Integer currentI = 0;
	private Integer maxK = 0;
	private Integer currentJ = 0;
	private Integer maxJ = 0;
	private MapKeyIn key;
	private Text value = new Text();

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		FileSplit fileSplit = (FileSplit) inputSplit;
		final Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(file);
		in = new BufferedReader(new InputStreamReader(fileIn));
		in.mark(8192);
		findMaxIandK();
		in.reset();
	}

	private void findMaxIandK() throws IOException {
		String nextRow = in.readLine();
		
		while (nextRow != null && !nextRow.trim().isEmpty()) {
			maxI++;
			nextRow = in.readLine();
		}
		
		String row_k = in.readLine();
		if (row_k == null) return;
		Pattern nonwhitespace = Pattern.compile("\\S+");
		Matcher m = nonwhitespace.matcher(row_k);
		while (m.find()) {
			maxK++;
		}
		maxJ++;
		
		while (nextRow != null && !nextRow.trim().isEmpty()) {
			maxJ++;
			nextRow = in.readLine();
		}
	}

	@Override
	public void close() throws IOException {
		in.close();

	}

	@Override
	public MapKeyIn getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (maxI == null || maxJ == null || currentI == null || currentJ == null) 
			return 0.0f;
		
		if (maxI + maxJ == 0) {
			return 0.0f;
		}
		return ((currentI + currentJ) / (maxI + maxJ));
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String nextVal = in.readLine();
		
		// end of split
		if (nextVal == null) {
			currentMatrix = null;
			maxI = null;
			maxK = null;
			value = null;
			
			return false;
		} else if (nextVal.isEmpty()) {
			nextVal = in.readLine();
			currentMatrix = N;
		}

		// end of split
		if (nextVal == null || nextVal.isEmpty()) {
			currentMatrix = null;
			maxI = null;
			maxK = null;
			value = null;
			
			return false;
		}
		
		if (currentMatrix == M) {
			currentI++;
			key = MapKeyIn.createMKey(currentI, maxK);
		}
		else if (currentMatrix == N) {
			currentJ++;
			key = MapKeyIn.createNKey(maxI, currentJ);
		}
		
		value = new Text(nextVal);
		
		return true;
	}
}
