package splittablecodec;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

public class CodecInputStream extends SplitCompressionInputStream {
	
	private int idx_block_size;
	private Decompressor decompressor;
	private long pos;
	
	private long[] block_sizes_split;
	private int num_blocks_split;
	
	private byte[] compressed_block_buffer;
	
	public CodecInputStream(InputStream in, Decompressor decompressor, long start, long end, Configuration conf) throws IOException {
		super(in, start, end);
		compressed_block_buffer = new byte[Integer.parseInt(conf.get("compressed_buffer_size"))];
		pos = start;
		this.decompressor = decompressor;
		
		String[] blocksizes_strings = conf.get("blocksizes").substring(1, conf.get("blocksizes").length()-1).split(", ");
		long[] blocksizes = new long[blocksizes_strings.length];
		
		for (int i = 0; i < blocksizes.length; i++)
			blocksizes[i] = Long.parseLong(blocksizes_strings[i]);
		
		long block_start = conf.getLong("header_size", 0);
		block_sizes_split = new long[blocksizes.length];
		int i = 0;
		
		while(block_start < end) {
			
			if (block_start >= start)
				block_sizes_split[num_blocks_split++] = blocksizes[i];
			
			block_start += blocksizes[i++];
		}

		int[] params = conf.getInts("parameters");

		if (params != null) {
			byte[] parameters = new byte[params.length];

			for (int j=0; j < parameters.length; j++)
				parameters[j] = (byte) params[j];

			NativeCodecDecompressor.setParameters(parameters);
		}
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {	
		
		int n = 0;
        while ((n = decompressor.decompress(b, off, len)) == 0) {
            if (decompressor.finished() && idx_block_size >= num_blocks_split) {
            	pos += block_sizes_split[idx_block_size-1];
            	return -1;
            }

            if (decompressor.needsInput()) {
            	int requested_bytes = (int) block_sizes_split[idx_block_size];
            	int offset = 0;
            	int m;
            	
            	while (requested_bytes > 0) {
            		m = in.read(compressed_block_buffer, offset, requested_bytes);
            		requested_bytes -= m;
            		offset += m;
            	}
            	
            	pos += idx_block_size == 0 ? 0 : block_sizes_split[idx_block_size-1];
                decompressor.setInput(compressed_block_buffer, 0, (int) block_sizes_split[idx_block_size++]);
            }
        }
		
		return n;
	}
	
	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public int read() throws IOException {
		byte[] b = new byte[1];
		int result = read(b, 0, 1);
		
		return (result > 0 ? b[0] : result);
	}

	@Override
	public void resetState() throws IOException {
		// nop
	}
}
