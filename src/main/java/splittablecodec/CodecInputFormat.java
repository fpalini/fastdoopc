package splittablecodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.primitives.Ints;

public abstract class CodecInputFormat<K, V> extends FileInputFormat<K, V>{

	private byte[] parameters;
	private Long[] block_sizes;

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> defaultSplits = super.getSplits(job);
        List<InputSplit> codecSplits = new ArrayList<>();
        
        FileSplit split = (FileSplit) defaultSplits.get(0);
		Path path = split.getPath();
		
		FSDataInputStream archive = path.getFileSystem(job.getConfiguration()).open(path);
        long fileLen = path.getFileSystem(job.getConfiguration()).getFileStatus(path).getLen();

		extractCodecInfo(archive, fileLen, job.getConfiguration());
		
		addCodecParameters(job.getConfiguration());
		
		long start_pos = getStartData(); // header_size
		
		int bs_index = 0;
		
		long split_size;
		
		for (InputSplit defaultSplit : defaultSplits) {
			
			try {
				split_size = 0;
				
				while (bs_index < block_sizes.length && split_size <= defaultSplit.getLength())
					split_size += block_sizes[bs_index++];		
				
				if (split_size == 0) break;	
				
				codecSplits.add(new FileSplit(path, start_pos, split_size, defaultSplit.getLocations()));
				
				start_pos += split_size;
			
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
		
		if (codecSplits.isEmpty())
			codecSplits = defaultSplits;
		
        return codecSplits;
	}
	
	abstract public int getStartData();
	abstract public void extractCodecInfo(FSDataInputStream archive, long fileLen, Configuration configuration) throws IOException;
	
	private void addCodecParameters(Configuration configuration) {
		configuration.set("compressed_buffer_size", Collections.max(Arrays.asList(getBlocksizes())).toString());
		configuration.set("blocksizes", Arrays.toString(getBlocksizes()));
		configuration.setLong("header_size", getStartData());

		if (parameters != null) {
			String[] params = new String[parameters.length];

			for (int i=0; i < params.length; i++)
				params[i] = ""+parameters[i];

			configuration.setStrings("parameters", params);
		}
	}

	public void extractBlockSizes(byte[] array, int start, int num_blocks) {
		extractBlockSizes(array, start, num_blocks, true);
	}
	
	public void extractBlockSizes(byte[] array, int start, int num_blocks, boolean bigEndian) {
		block_sizes = new Long[num_blocks];
		
		for (int b = 0; b < num_blocks; b++)
			if (bigEndian)
				block_sizes[b] = (long) Ints.fromBytes(array[4*b+start], array[4*b+(start+1)], array[4*b+(start+2)], array[4*b+(start+3)]);
			else
				block_sizes[b] = (long) Ints.fromBytes(array[4*b+(start+3)], array[4*b+(start+2)], array[4*b+(start+1)], array[4*b+start]);
	}

	public void setParameters(byte[] parameters) {
		this.parameters = parameters;
	}
	
	public Long[] getBlocksizes() {
		return block_sizes;
	}
}
