package fastdoop.compression;

import java.io.IOException;

import dsrc.DsrcInputFormat;
import fastdoop.QRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FASTQDSRCInputFormat extends DsrcInputFormat<Text, QRecord> {

	@Override
	public RecordReader<Text, QRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		return new FASTQReadsRecordReader();
	}

}
