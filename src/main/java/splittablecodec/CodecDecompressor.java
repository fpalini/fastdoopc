package splittablecodec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class CodecDecompressor implements Decompressor {

	private final int compressed_buffer_size;
	private final int uncompressed_buffer_size;
	private ByteBuffer uncompressed, compressed;

	public CodecDecompressor(Configuration conf) {
		compressed_buffer_size = Integer.parseInt(conf.get("compressed_buffer_size"));
		uncompressed_buffer_size = conf.get("uncompressed_buffer_size") == null ? 20 * compressed_buffer_size : Integer.parseInt(conf.get("uncompressed_buffer_size"));

		uncompressed = ByteBuffer.allocate(uncompressed_buffer_size);
		uncompressed.limit(uncompressed_buffer_size);
		uncompressed.position(uncompressed_buffer_size);

		compressed = ByteBuffer.allocate(compressed_buffer_size);
	    compressed.limit(compressed_buffer_size);
	    compressed.position(compressed_buffer_size);
	}

	@Override
	public int decompress(byte[] b, int off, int len) throws IOException {
		long startDecompress = System.currentTimeMillis();
		
	    if (b == null) {
	      throw new NullPointerException();
	    }
	    if (off < 0 || len < 0 || off > b.length - len) {
	      throw new ArrayIndexOutOfBoundsException();
	    }
	    
	    int n = uncompressed.remaining();
	    
	    // If there are uncompressed byte not returned, return them
	    if (n > 0) {
	      n = Math.min(n, len);
	      uncompressed.get(b, off, n);
	      return n;
	    }
	    
	    // If there is no buffered data, decompress new available compressed data
	    
	    uncompressed.rewind();
	    uncompressed.limit(uncompressed_buffer_size);
	    
	    // long timeBash = 0;
	    
	    if (compressed.hasRemaining()) {
		    byte[] input = new byte[compressed.remaining()];
		    
	    	compressed.get(input);
			
			// long startBash = System.currentTimeMillis();
	    	
		    byte[] output = decompress(input);

		    n = output.length;

			// timeBash = System.currentTimeMillis() - startBash;
		    	
	    	uncompressed.put(output, 0, n);
	    }
	    else {
	    	n = 0;
	    }
	    
	    uncompressed.limit(n);
	    uncompressed.rewind();

	    n = Math.min(n, len);
	    uncompressed.get(b, off, n);
	    
		// System.out.println("Decompress Time(s): "+((System.currentTimeMillis()-startDecompress)/1000.0) + "   Bash Time(s): "+(timeBash/1000.0));

	    return n;
	}

	public abstract byte[] decompress(byte[] input);

	@Override
	public void end() {
		// nop
	}

	@Override
	public boolean finished() {
		return !uncompressed.hasRemaining() && !compressed.hasRemaining();
	}

	@Override
	public int getRemaining() {
		return uncompressed.remaining();
	}

	@Override
	public boolean needsDictionary() {
		return false;
	}

	@Override
	public boolean needsInput() {
		return !uncompressed.hasRemaining();
	}

	@Override
	public void reset() {
		// nop
	}

	@Override
	public void setDictionary(byte[] arg0, int arg1, int arg2) {
		// nop
	}

	@Override
	public void setInput(byte[] b, int off, int len) {
		if (b == null) {
		      throw new NullPointerException();
	    }
	    if (off < 0 || len < 0 || off > b.length - len) {
	      throw new ArrayIndexOutOfBoundsException();
	    }
	    
	    /*if (len > compressed.limit())
	    	throw new BufferOverflowException();*/
	  
	    compressed.limit(compressed_buffer_size);
	    compressed.rewind();
	    compressed.put(b, off, len);
	    compressed.position(off);
	    compressed.limit(off+len);
	    
	    // Reinitialize output direct buffer.
	    uncompressed.limit(uncompressed_buffer_size);
	    uncompressed.position(uncompressed_buffer_size);
	}
}
