package fastdoop.compression;

import fastdoop.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import universalcodec.UniversalInputFormat;

public class FASTAShortUniversalInputFormat extends UniversalInputFormat<Text, Record>{

	@Override
	public RecordReader<Text, Record> createRecordReader(InputSplit split, TaskAttemptContext context) {
		
		return new ShortReadsRecordReader();
	}
}
