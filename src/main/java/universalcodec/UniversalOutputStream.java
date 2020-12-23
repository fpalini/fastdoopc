package universalcodec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.spark_project.guava.primitives.Ints;
import org.spark_project.guava.primitives.Longs;


public class UniversalOutputStream extends CompressionOutputStream {

	private UniversalCompressor compressor;
	private ByteBuffer uncompressed;
	private ArrayList<Integer> blocksizes = new ArrayList<Integer>();
	private byte[] in_data, out_data;

	public UniversalOutputStream(OutputStream out, UniversalCompressor compressor) {
		super(out);
		this.compressor = compressor;
		uncompressed = ByteBuffer.allocate(UniversalCodec.UNCMP_BUFFER_SIZE);
		uncompressed.limit(UniversalCodec.UNCMP_BUFFER_SIZE);
		
		in_data = new byte[UniversalCodec.UNCMP_BUFFER_SIZE];
		out_data = new byte[UniversalCodec.CMP_BUFFER_SIZE];
		
		try {
				out.write(0xA1);
				out.write(DigestUtils.md5(compressor.algo.name));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void finish() throws IOException {
		if (uncompressed.position() > 0) {
			int len = uncompressed.position();
			
			uncompressed.rewind();
			uncompressed.get(in_data);
			
			compressor.setInput(in_data, 0, len);
			
			int compressed_size = compressor.compress(out_data, 0, out_data.length);
			
			blocksizes.add(compressed_size);
			
			out.write(out_data, 0, compressed_size);
		}
		
		out.write(0xA2);
		
		for (int bs : blocksizes)
			out.write(Ints.toByteArray(bs));
		
		long tot_sizes = 0;
		
		for (int bs : blocksizes)
			tot_sizes += bs;
		
		out.write(0xFF);
		
		// System.out.println("BLOCKS: " + blocksizes);
		
		out.write(Ints.toByteArray(blocksizes.size()));
		out.write(Longs.toByteArray(UniversalCodec.HEADER_SIZE + tot_sizes));
	}

	@Override
	public void resetState() throws IOException {
		// nop
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		for (int i = off; i < len; i++)
			write(b[i]);
	}
	
	private char prevC = '\0';

	@Override
	public void write(int b) throws IOException {
		char b_c = (char)b;
		
		if (uncompressed.position() < UniversalCodec.UNCMP_BLOCK_SIZE) {
			uncompressed.put((byte) b);
			prevC = b_c;
		}
		else {
			
			byte startChar = UniversalCompressorUtility.delimiter(compressor.algo);
			
			if (startChar != '\0') {
				if (prevC != '\n' || b_c != startChar) {
					uncompressed.put((byte)b);
					prevC = b_c;
				}
				else {
					int pos = uncompressed.position();
					uncompressed.rewind();
					uncompressed.get(in_data);
					uncompressed.rewind();
					
					compressor.setInput(in_data, 0, pos);
					
					int compressed_size = compressor.compress(out_data, 0, out_data.length);
					
					blocksizes.add(compressed_size);
					
					out.write(out_data, 0, compressed_size);
					
					uncompressed.put((byte)b);
					prevC = b_c;
				}
			}
			else {
				int pos = uncompressed.position();
				uncompressed.rewind();
				uncompressed.get(in_data);
				uncompressed.rewind();
				
				compressor.setInput(in_data, 0, pos);
				
				int compressed_size = compressor.compress(out_data, 0, out_data.length);
				
				blocksizes.add(compressed_size);
				
				out.write(out_data, 0, compressed_size);
			}
		}
	}
}
