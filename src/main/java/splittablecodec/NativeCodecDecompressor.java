package splittablecodec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.collections.BufferOverflowException;

public class NativeCodecDecompressor implements Decompressor {
	
	private ByteBuffer compressedDirectBuf, uncompressedDirectBuf;
	private int compressedBufferLimit, uncompressedBufferLimit;
	private int compressedDirectBufOff, compressedDirectBufLen;

	static {
		initIDs();
	}

    public NativeCodecDecompressor(Configuration conf) {
		compressedBufferLimit = Integer.parseInt(conf.get("compressed_buffer_size"));
        uncompressedBufferLimit = conf.get("uncompressed_buffer_size") == null ? 20 * compressedBufferLimit : Integer.parseInt(conf.get("uncompressed_buffer_size"));
        compressedDirectBuf = ByteBuffer.allocateDirect(compressedBufferLimit);
        uncompressedDirectBuf = ByteBuffer.allocateDirect(uncompressedBufferLimit);
        uncompressedDirectBuf.position(uncompressedBufferLimit);
	}

	public void setInput(byte[] b, int off, int len) {
		if (b == null) {
			throw new NullPointerException();
		}
		if (off < 0 || len < 0 || off > b.length - len) {
			throw new ArrayIndexOutOfBoundsException();
		}

		if (len > compressedBufferLimit)
			throw new BufferOverflowException();

		compressedDirectBuf.rewind();
		compressedDirectBuf.put(b, off, len);
		compressedDirectBufLen = len;

		// Reinitialize output direct buffer.
		uncompressedDirectBuf.limit(uncompressedBufferLimit);
		uncompressedDirectBuf.position(uncompressedBufferLimit);
	}

	public boolean needsInput() {
		// Consume remaining compressed data?
		if (uncompressedDirectBuf.remaining() > 0) {
			return false;
		}

		return true;
	}

	public void setDictionary(byte[] b, int off, int len) {
		// nothing to do
	}

	public boolean needsDictionary() {
		return false;
	}

	public boolean finished() {
		return uncompressedDirectBuf.remaining() == 0; // and all the blocks are decompressed (defined outside)
	}

	public int decompress(byte[] b, int off, int len) throws IOException {

		if (b == null) {
			throw new NullPointerException();
		}
		if (off < 0 || len < 0 || off > b.length - len) {
			throw new ArrayIndexOutOfBoundsException();
		}

		// Check if there is uncompressed data.
		int n = uncompressedDirectBuf.remaining();
		if (n > 0) {
			n = Math.min(n, len);
			uncompressedDirectBuf.get(b, off, n);

			return n;
		}

		if (compressedDirectBufLen > 0) {

			// Re-initialize output direct buffer.
			uncompressedDirectBuf.rewind();
			uncompressedDirectBuf.limit(uncompressedBufferLimit);

			// Decompress the data.
			try{ n = decompress(); } catch (Throwable t) { t.printStackTrace(); }

			uncompressedDirectBuf.limit(n);

			// Get at most 'len' bytes.
			n = Math.min(n, len);

			uncompressedDirectBuf.get(b, off, n);

			compressedDirectBufLen = 0; // TODO to check
		}

		return n;
	}

	public int getRemaining() {
		return compressedDirectBufLen;
	}

	public void reset() {
		compressedDirectBufOff = compressedDirectBufLen = 0;
		uncompressedDirectBuf.limit(uncompressedBufferLimit);
		uncompressedDirectBuf.position(uncompressedBufferLimit);
	}

	public void end() {
		// nothing to do
	}

	private native static void initIDs();
	private native int decompress();
	public native static void setParameters(byte[] parameters);
}

