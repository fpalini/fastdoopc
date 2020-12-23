package dsrc;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import splittablecodec.CodecInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Arrays;

public abstract class DsrcInputFormat<K, V> extends CodecInputFormat<K, V> {
	
	public static int HEADER_SIZE = 40;
	
	@Override
	public void extractCodecInfo(FSDataInputStream archive, long fileLen, Configuration configuration) throws IOException {
		
		byte[] header = new byte[HEADER_SIZE];
		
		archive.read(0, header, 0, HEADER_SIZE);
			
		int footer_size = Ints.fromBytes(header[4], header[5], header[6], header[7]);
		
		long footer_offset = Longs.fromBytes(header[8], header[9], header[10], header[11], header[12], header[13], header[14], header[15]);
		
		long num_blocks = Longs.fromBytes(header[24], header[25], header[26], header[27], header[28], header[29], header[30], header[31]);
		
		byte[] footer = new byte[footer_size]; // TODO change buffer management
		
		archive.read(footer_offset, footer, 0, footer_size);
		
		extractBlockSizes(footer, 1, (int) num_blocks, false);
		
		int param_index = 1 + getBlocksizes().length * 4;

		setParameters(Arrays.copyOfRange(footer, param_index, param_index+13));
	}
	
	@Override
	public int getStartData() {
		return HEADER_SIZE;
	}
}
