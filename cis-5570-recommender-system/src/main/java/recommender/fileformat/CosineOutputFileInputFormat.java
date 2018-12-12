package recommender.fileformat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


public class CosineOutputFileInputFormat extends FileInputFormat {

	@Override
	public RecordReader createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {

		return new CosineOutputRecordReader();
	}

}
