package fastdoop.compression;

import fastdoop.QRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import universalcodec.UniversalInputFormat;

public class FASTQUniversalInputFormat extends UniversalInputFormat<Text, QRecord>{

	@Override
	public RecordReader<Text, QRecord> createRecordReader(InputSplit split, TaskAttemptContext context) {
		
		return new FASTQReadsRecordReader();
	}
}
