package universalcodec;

import org.apache.hadoop.conf.Configuration;
import splittablecodec.CodecCompressor;

public class UniversalCompressor extends CodecCompressor {

	public final Algo algo;

	public UniversalCompressor(Configuration conf) {
		super(conf);
		algo = Algo.create(conf);
	}

	@Override
	public byte[] compress(byte[] input) {
		return algo.compress(input);
	}
}
