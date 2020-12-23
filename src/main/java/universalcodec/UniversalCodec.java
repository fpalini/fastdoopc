package universalcodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import splittablecodec.CodecInputStream;

public class UniversalCodec implements Configurable, SplittableCompressionCodec {

	public static final int HEADER_SIZE = 1 + 16;
	public static final int UNCMP_BLOCK_SIZE = 128 * 1024 * 1024; // Size of the uncompressed block of data (Limited to 144Kb for stdin/stdout)
	public static final int UNCMP_BUFFER_SIZE = UNCMP_BLOCK_SIZE + 2048; // Size of the buffer for uncompressed data (should be greater than UNCMP_BLOCK_SIZE because of the FASTQ/A alignment)
	public static final int CMP_BUFFER_SIZE = UNCMP_BUFFER_SIZE; // Size of the buffer for compressed data (should be less or equal than UNCMP_BLOCK_SIZE based on the compressing ratio)

	private Configuration conf;

	@Override
	public Compressor createCompressor() {
		return new UniversalCompressor(conf);
	}

	@Override
	public Decompressor createDecompressor() {
		return new UniversalDecompressor(conf);
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in) throws IOException {
		conf.setInt("compressed_buffer_size", CMP_BUFFER_SIZE);
		conf.setInt("uncompressed_buffer_size", UNCMP_BUFFER_SIZE);

		return new CodecInputStream(in, new UniversalDecompressor(conf), 0, -1, conf);
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
		conf.setInt("compressed_buffer_size", CMP_BUFFER_SIZE);
		conf.setInt("uncompressed_buffer_size", UNCMP_BUFFER_SIZE);

		return new CodecInputStream(in, (UniversalDecompressor) decompressor, 0, Long.MAX_VALUE, conf);
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
		return new UniversalOutputStream(out, new UniversalCompressor(conf));
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
		return new UniversalOutputStream(out, (UniversalCompressor) compressor);
	}

	@Override
	public Class<? extends Compressor> getCompressorType() {
		return UniversalCompressor.class;
	}

	@Override
	public Class<? extends Decompressor> getDecompressorType() {
		return UniversalDecompressor.class;
	}

	@Override
	public String getDefaultExtension() {
		return ".uc";
	}

	@Override
	public SplitCompressionInputStream createInputStream(InputStream in, Decompressor decompressor, long start,
														 long end, READ_MODE mode) throws IOException {
		conf.setInt("compressed_buffer_size", CMP_BUFFER_SIZE);
		conf.setInt("uncompressed_buffer_size", UNCMP_BUFFER_SIZE);

		return new CodecInputStream(in, decompressor, start, end, conf);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public InputStream createInputStream(FSDataInputStream in, long startByte, long endByte)
			throws IOException {
		conf.setInt("compressed_buffer_size", CMP_BUFFER_SIZE);
		conf.setInt("uncompressed_buffer_size", UNCMP_BUFFER_SIZE);

		return new CodecInputStream(in, new UniversalDecompressor(conf), startByte, endByte, conf);
	}

	public InputStream createInputStream(FSDataInputStream in, Decompressor decompressor, long startByte,
												  long endByte, READ_MODE mode) throws IOException {
		conf.setInt("compressed_buffer_size", CMP_BUFFER_SIZE);
		conf.setInt("uncompressed_buffer_size", UNCMP_BUFFER_SIZE);

		return new CodecInputStream(in, decompressor, startByte, endByte, conf);
	}
}
