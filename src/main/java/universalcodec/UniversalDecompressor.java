package universalcodec;

import org.apache.hadoop.conf.Configuration;
import splittablecodec.CodecDecompressor;

public class UniversalDecompressor extends CodecDecompressor {

	private final Algo algo;

	public UniversalDecompressor(Configuration conf) {
		super(conf);
		algo = Algo.create(conf);
	}

	@Override
	public byte[] decompress(byte[] input) {
		return algo.decompress(input);
	}
}
