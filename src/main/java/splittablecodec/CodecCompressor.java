package splittablecodec;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import universalcodec.Algo;
import universalcodec.UniversalCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class CodecCompressor implements Compressor {

	private final int compressed_buffer_size;
	private final int uncompressed_buffer_size;
	private ByteBuffer uncompressed, compressed;
	private long bytesRead;
	private long bytesWritten;

	public CodecCompressor(Configuration conf) {
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
	public int compress(byte[] b, int off, int len) throws IOException {
		long startCompress = System.currentTimeMillis();
		
		if (b == null) {
	      throw new NullPointerException();
	    }
	    if (off < 0 || len < 0 || off > b.length - len) {
	      throw new ArrayIndexOutOfBoundsException();
	    }
	    
	    int n = compressed.remaining();
	    
	    // If there are uncompressed byte not returned, return them
	    if (n > 0) {
	      n = Math.min(n, len);
	      compressed.get(b, off, n);
	      bytesWritten += n;
	      return n;
	    }
	    
	    // If there is no buffered data, decompress new available compressed data
	    
	    compressed.rewind();
	    compressed.limit(compressed_buffer_size);
	    
	    byte[] input = new byte[uncompressed.remaining()];
	    
	    // long timeBash = 0;
	    
	    if (uncompressed.hasRemaining()) {
	    	uncompressed.get(input);
	    	bytesRead += input.length;
	    	
	    	// long startBash = System.currentTimeMillis();
	    	
		    byte[] output = compress(input);

	    	n = output.length;
			
			// timeBash = System.currentTimeMillis() - startBash;
		    
	    	compressed.put(output, 0, n);
	    }
	    else {
	    	n = 0;
	    }
	    
	    compressed.limit(n);
	    compressed.rewind();

	    n = Math.min(n, len);
	    compressed.get(b, off, n);
	    bytesWritten += n;
	    
	    // System.out.println("Compress Time(s): "+((System.currentTimeMillis()-startCompress)/1000.0) + "   Bash Time(s): "+(timeBash/1000.0));
	    
	    return n;
	}

	protected abstract byte[] compress(byte[] input);

	@Override
	public void end() {
		// nop 
	}

	@Override
	public void finish() {
		// nop
	}

	@Override
	public boolean finished() {
		return !uncompressed.hasRemaining() && !compressed.hasRemaining();
	}

	@Override
	public long getBytesRead() {
		return bytesRead;
	}

	@Override
	public long getBytesWritten() {
		return bytesWritten;
	}

	@Override
	public boolean needsInput() {
		return !compressed.hasRemaining();
	}

	@Override
	public void reinit(Configuration conf) {
		// nop
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
	    
	    /*if (len > uncompressed.limit())
	    	throw new BufferOverflowException();*/
	  
	    uncompressed.limit(uncompressed_buffer_size);
	    uncompressed.rewind();
	    uncompressed.put(b, off, len);
	    uncompressed.position(off);
	    uncompressed.limit(off+len);
	    
	    // Reinitialize output direct buffer.
	    compressed.limit(compressed_buffer_size);
	    compressed.position(compressed_buffer_size);
	}
}
